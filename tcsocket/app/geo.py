import hashlib
import json

from .settings import Settings
from .utils import HTTPTooManyRequestsJson

ONE_HOUR = 3_600
NINETY_DAYS = ONE_HOUR * 24 * 90
IP_HEADER = 'X-Forwarded-For'


def get_ip(request):
    ips = request.headers.get(IP_HEADER)
    return ips and ips.split(',', 1)[0].strip(' ')


async def geocode(request):
    location_str = request.GET.get('location')
    if not location_str:
        return

    location_str = location_str.strip(' \t\n\r,.')
    loc_key = 'loc:' + hashlib.md5(location_str.encode()).hexdigest()
    redis_pool = request.app['redis']
    settings: Settings = request.app['settings']

    ip_address = get_ip(request)
    assert ip_address, 'missing header "X-Forwarded-For"'
    with await redis_pool as redis:
        loc_data = await redis.get(loc_key)
        if loc_data:
            return json.loads(loc_data.decode())

        ip_key = 'geoip:' + ip_address
        geo_attempts = int(await redis.incr(ip_key))
        if geo_attempts == 1:
            # set expires on the first attempt
            await redis.expire(ip_key, ONE_HOUR)
        elif geo_attempts > 10:
            raise HTTPTooManyRequestsJson(
                status='too_many_requests',
                details='to many geocoding requests submitted',
            )
        params = {
            'address': location_str,
            'key': settings.geocoding_key,
        }
        data = None
        async with request.app['session'].get(settings.geocoding_url, params=params) as r:
            try:
                # 400 if the address is invalid
                assert r.status in {200, 400}
                data = await r.json()
            except (ValueError, AssertionError) as e:
                body = await r.read()
                raise RuntimeError(f'Bad response from {settings.geocoding_url} {r.status}, response:\n{body}') from e

        results = data['results']
        if results:
            result = {
                'pretty': results[0]['formatted_address'],
                'lat': results[0]['geometry']['location']['lat'],
                'lng': results[0]['geometry']['location']['lng'],
            }
        else:
            result = None
        await redis.setex(loc_key, NINETY_DAYS, json.dumps(result).encode())
        return result

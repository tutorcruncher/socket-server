import hashlib
import json

from .settings import Settings
from .utils import HTTPTooManyRequestsJson

NINETY_DAYS = 3600 * 24 * 90
IP_HEADER = 'X-Forwarded-For'


def get_ip(request):
    ips = request.headers.get(IP_HEADER)
    assert ips, 'missing header "X-Forwarded-For"'
    return ips.split(',', 1)[0]


async def geocode(request):
    location_str = request.GET.get('location')
    if not location_str:
        return

    location_str = location_str.strip(' \t\n\r,.')
    loc_ref = 'loc:' + hashlib.md5(location_str.encode()).hexdigest()
    redis_pool = await request.app['redis']
    settings: Settings = request.app['settings']
    with await redis_pool as redis:
        loc_data = await redis.get(loc_ref)
        if loc_data:
            return json.loads(loc_data.decode())

        ip_key = b'geoip:%s' % get_ip(request).encode()
        geo_attempts = int(await redis.incr(ip_key))
        if geo_attempts == 1:
            # set expires on the first attempt
            await redis.expire(ip_key, 3600)
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
                # 400 if the location is invalid
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
        await redis.setex(loc_ref, NINETY_DAYS, json.dumps(result).encode())
        return result

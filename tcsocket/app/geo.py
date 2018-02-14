import hashlib
import json
import logging

from .settings import Settings
from .utils import HTTPTooManyRequestsJson

ONE_HOUR = 3_600
NINETY_DAYS = ONE_HOUR * 24 * 90
IP_HEADER = 'X-Forwarded-For'
COUNTRY_HEADER = 'CF-IPCountry'
logger = logging.getLogger('socket.geo')


def get_ip(request):
    ips = request.headers.get(IP_HEADER)
    return ips and ips.split(',', 1)[0].strip(' ')


async def geocode(request):
    location_str = request.GET.get('location')
    if not location_str:
        return

    location_str = location_str.strip(' \t\n\r,.')
    attempts = [
        {'address': location_str, 'components': f'country:{request.headers[COUNTRY_HEADER]}'},
        {'address': location_str},
    ]
    for params in attempts:
        r = await _geocode(request, params)
        if r:
            return r


async def _geocode(request, params):
    cache_ref = json.dumps(params, sort_keys=True).encode()
    loc_key = 'loc:' + hashlib.md5(cache_ref).hexdigest()
    redis_pool = request.app['redis']
    settings: Settings = request.app['settings']

    ip_address = get_ip(request)
    assert ip_address, 'missing header "X-Forwarded-For"'
    with await redis_pool as redis:
        loc_data = await redis.get(loc_key)
        if loc_data:
            result = json.loads(loc_data.decode())
            logger.info('cached geocode result "%s" > "%s"', params, result and result['pretty'])
            return result

        ip_key = 'geoip:' + ip_address
        geo_attempts = int(await redis.incr(ip_key))
        if geo_attempts == 1:
            # set expires on the first attempt
            await redis.expire(ip_key, ONE_HOUR)
        elif geo_attempts > 10:
            logger.warning('%d geocode attempts from "%s" in the last hour', geo_attempts, ip_address)
            raise HTTPTooManyRequestsJson(
                status='too_many_requests',
                details='to many geocoding requests submitted',
            )
        get_params = dict(**params, key=settings.geocoding_key)
        data = None
        async with request.app['session'].get(settings.geocoding_url, params=get_params) as r:
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
        logger.info('new geocode result "%s" > "%s" (%d from "%s")',
                    params, result and result['pretty'], geo_attempts, ip_address)
        return result

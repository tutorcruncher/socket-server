import hashlib
import json
import logging

from .settings import Settings

ONE_HOUR = 3_600
NINETY_DAYS = ONE_HOUR * 24 * 90
IP_HEADER = 'X-Forwarded-For'
COUNTRY_HEADER = 'CF-IPCountry'
MAX_GEOCODE_PER_HOUR = 20
logger = logging.getLogger('socket.geo')


def get_ip(request):
    ips = request.headers.get(IP_HEADER)
    return ips and ips.split(',', 1)[0].strip(' ')


async def geocode(request):
    location_str = request.query.get('location')
    if not location_str:
        return

    location_str = location_str.strip(' \t\n\r,.')
    region = request.headers[COUNTRY_HEADER].lower()
    if region == 'gb':
        # https://en.wikipedia.org/wiki/Country_code_top-level_domain#ASCII_ccTLDs_not_in_ISO_3166-1
        region = 'uk'

    loc_ref = f'{location_str.lower()}|{region}'
    loc_key = f'loc:{hashlib.md5(loc_ref.encode()).hexdigest()[:8]}:{loc_ref[:50]}'
    redis_pool = request.app['redis']
    settings: Settings = request.app['settings']

    ip_address = get_ip(request)
    assert ip_address, 'missing header "X-Forwarded-For"'
    with await redis_pool as redis:
        loc_data = await redis.get(loc_key)
        if loc_data:
            result = json.loads(loc_data)
            logger.info(
                'cached geocode result "%s|%s" > "%s"', location_str, region, result.get('error') or result['pretty']
            )
            return result

        ip_key = 'geoip:' + ip_address
        geo_attempts = int(await redis.incr(ip_key))
        if geo_attempts == 1:
            # set expires on the first attempt
            await redis.expire(ip_key, ONE_HOUR)
        elif geo_attempts > MAX_GEOCODE_PER_HOUR:
            logger.warning('%d geocode attempts from "%s" in the last hour', geo_attempts, ip_address)
            return {'error': 'rate_limited'}
        params = {
            'address': location_str,
            'region': region,
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
            result = {'error': 'no_results'}
        await redis.setex(loc_key, NINETY_DAYS, json.dumps(result).encode())
        logger.info(
            'new geocode result "%s|%s" > "%s" (%d from "%s")',
            location_str,
            region,
            result.get('error') or result['pretty'],
            geo_attempts,
            ip_address,
        )
        return result

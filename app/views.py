from aiohttp.web_reqrep import json_response


async def index(request):
    """
    This is the view handler for the "/" url.

    :param request: the request object see http://aiohttp.readthedocs.io/en/stable/web_reference.html#request
    :return: context for the template.
    """
    return json_response({
        'title': request.app['name'],
        'intro': "Success! you've setup a basic aiohttp app.",
    })

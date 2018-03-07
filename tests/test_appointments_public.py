

async def test_list_appointments(cli, company, appointment):
    r = await cli.get(cli.server.app.router['appointment-list'].url_for(company='thepublickey'))
    assert r.status == 200, await r.text()
    obj = await r.json()
    assert obj == {
        'results': [
            {
                'link': '456-testing-appointment',
                'topic': 'testing appointment',
                'attendees_max': 42,
                'attendees_count': 4,
                'start': '2032-01-01T12:00:00',
                'finish': '2032-01-01T13:00:00',
                'price': 123.45,
                'location': 'Whatever',
                'service_id': 123,
                'service_name': 'testing service',
                'service_colour': '#abc',
                'service_extra_attributes': [
                    {
                        'name': 'Foobar',
                        'type': 'text_short',
                        'machine_name': 'foobar',
                        'value': 'this is the value of foobar',
                    }
                ]
            },
        ],
        'count': 1,
    }

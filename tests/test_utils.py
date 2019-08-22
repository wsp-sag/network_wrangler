import os
import pytest



slug_test_list = [
    {'text':"I am a roadway", 'delim':"_", "answer": "i_am_a_roadway"},
    {'text':"I'm a roadway", 'delim':"_", "answer": "im_a_roadway"},
    {'text':"I am a roadway", 'delim':"-", "answer": "i-am-a-roadway"},
    {'text':"I am a roadway", 'delim':"", "answer": "iamaroadway"},
]

@pytest.mark.parametrize("slug_test", slug_test_list)
def test_get_slug(request, slug_test):
    print("\n--Starting:",request.node.name)
    from network_wrangler.Utils import make_slug
    slug = make_slug(slug_test['text'],delimiter=slug_test['delim'])
    print('From: {} \nTo: {}'.format(slug_test['text'],slug))
    print('Expected: {}'.format(slug_test['answer']))
    assert(slug == slug_test['answer'])

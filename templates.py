from typing import Callable
from typing import Set
from enum import Enum
from pathlib import Path
import textwrap
import jinja2 as jinja


GLOBAL_TEMPLATE_FUNCTIONS = set[Callable]()

def template_function(function: Callable):
    # jinga_html_template.globals[function.__name__] = function
    GLOBAL_TEMPLATE_FUNCTIONS.add(function)
    return function


@template_function
def text_fill(text: str, width: int=80, indent: str='') -> str:
    return textwrap.fill(text, width=width, subsequent_indent=indent)


@template_function
def to_string(object) -> str:
    return str(object)

class TemplateFactory():

    ENVIRONMENT = jinja.Environment(loader=jinja.FileSystemLoader('./data/templates/'))
    TEMPLATES = dict[str, jinja.Template]()

    @classmethod
    def make_template(cls, template_name: str) -> jinja.Template:
        if template_name in cls.TEMPLATES:
            return cls.TEMPLATES[template_name]

        template = cls.ENVIRONMENT.get_template(template_name)
        for template_function in GLOBAL_TEMPLATE_FUNCTIONS:
            template.globals[template_function.__name__] = template_function

        cls.TEMPLATES[template_name] = template
        return template

    
def render_with(template: jinja.Template, context: dict) -> str:
    return template.render(context)
    


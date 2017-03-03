"""
Template section of the SD system. The Simple Data pipeliner system.

SDTemplateBase - This object uses jinja2 as the underlying template engine.
Initial Date: 21st Feb 2017
Written by Gary "The Colonel" Hodgson
"""

from jinja2 import meta, Environment
from jinja2.loaders import FileSystemLoader, PackageLoader

import os
import yaml


""" Credit to Josh: https://gist.github.com/joshbode/569627ced3076931b02f 
This allows inline file injection, if yaml/yml it will be processed
"""
class LoaderMeta(type):
    def __new__(metacls, __name__, __bases__, __dict__):
        """Add include constructer to class."""

        # register the include constructor on the class
        cls = super().__new__(metacls, __name__, __bases__, __dict__)
        cls.add_constructor('!include', cls.construct_include)

        return cls


class Loader(yaml.Loader, metaclass=LoaderMeta):
    """YAML Loader with `!include` constructor."""

    def __init__(self, stream):
        """Initialise Loader."""

        try:
            self._root = os.path.split(stream.name)[0]
        except AttributeError:
            self._root = os.path.curdir

        super().__init__(stream)

    def construct_include(self, node):
        """Include file referenced at node."""

        filename = os.path.abspath(os.path.join(
            self._root, self.construct_scalar(node)
        ))
        extension = os.path.splitext(filename)[1].lstrip('.')

        with open(filename, 'r') as f:
            if extension in ('yaml', 'yml'):
                return yaml.load(f, Loader)
            else:
                return ''.join(f.readlines())


""" End Loader """


class SDTemplateBaseException(Exception):
    pass


class SDTemplateMissingParametersException(Exception):
    """
    This exception object is used to raise template parameters missing error.
    It is not the responsibility of this exception to expose granularity... at the moment!
    """

    def __init__(self, missing_params):
        self.message = "SDTemplate requires the follow parameters: {}".format(missing_params)
        Exception.__init__(self, self.message)

        self._missing_params = missing_params

    @property
    def missing_parameters(self):
        return self._missing_params


class SDTemplateBase:
    """
    It is encouraged to derive from this object, though it is ok to use for generic purposes
    """

    def __init__(self, requires=None):
        """

        :param requires: A tuple that contains the required
        :type requires: tuple
        """

        if requires:
            assert isinstance(requires, tuple)

        self._req_props = requires

    def load(self, template_file):

        _yaml_dict = yaml.load(open(template_file, encoding='utf-8-sig'), Loader)

        if self._req_props:
            req_params = [i for i in self._req_props if i not in _yaml_dict.keys()]
            if len(req_params) > 0:
                raise SDTemplateMissingParametersException(missing_params=req_params)

        for k, v in _yaml_dict.items():
            self.__dict__[k] = v


class SDTemplateSQL(SDTemplateBase):
    """
    This is used for a SQL Template. It simply takes input values to render a template string.
    This object acts as an extended template, so that meta data can be added. This will help describe the sql.
    It is hoped to be extended to allow for better python object binding and other goodness.

    It should be allowed that input values can be cascaded from previous template calls
    """

    required_template_params = ("name", "description", "author", "dataquery")

    def __init__(self, templatefile=None, requires=required_template_params):
        super(SDTemplateSQL, self).__init__(requires)

        if templatefile:
            assert isinstance(templatefile, str)

            if not os.path.exists(templatefile):
                raise FileNotFoundError("File '{}' does not exist.".format(templatefile))

            self.load(templatefile)

            self.templatepath, self.filename = os.path.split(templatefile)

            try:
                self._dqname = self.dataquery['name']
                self._querytemplate = self.dataquery['querytemplate']
                self._queryparams = self.dataquery.get('queryparams', [])
                self._querytype = self.dataquery.get('querytype', 'undefined')

                self._init_querytemplatestring()
                for queryparam in self._queryparams:
                    name = queryparam.get('name')
                    value = queryparam.get('value')
                    if name and value:
                        self.update_inputs(**{name: value})

            except KeyError as ke:
                raise SDTemplateBaseException("Missing required key for SDTemplateSQL Data Template: {}".format(ke))


        else:
            raise SDTemplateBaseException("Not currently implemented must pass template file.")

    def _init_dq(self):
        """
        This will check to say that the minimum meta data is available
        :return: bool
        """




    """TODO: This is messy! """
    def get_query_param_info(self):
        """
        Use this function to return granular data for parameters
        :return: list depending
        """
        param_infos = []

        for input_key, input_value in self._input_vars.items():
            param_info = None
            for qry_p_name in self._queryparams:
                param_name = qry_p_name.get('name')
                param_desc = qry_p_name.get('desc',"No description available")
                param_value = qry_p_name.get('value')
                if param_name==input_key:
                    if input_value: # If a value has already been assigned then this will take precedence
                        param_value = input_value

                    param_info= {"name":param_name,"value":param_value,"desc":param_desc}
                    break
            if not param_info: # No parameter was matched to query params
                param_info = {"name": input_key, "value": input_value, "desc": "No description available"}
            param_infos.append(param_info)

        return param_infos


    def __call__(self, *args, **kwargs):
        """
        This is the object callable function.
        :param args:
        :param kwargs: Input variables that may be used prior to rendering
        :return: str (rendered template)
        """
        #self._validate()
        if kwargs:
            self.update_inputs(**kwargs)

        missing_inputs = self.invalid_inputs()
        if len(missing_inputs) > 0:
            raise SDTemplateMissingParametersException(missing_inputs)

        return self._render_template()


    @property
    def input_variables(self):
        """
        Function returns the input variables. This will give an indication of their current state.
        :return: list of dicts
        """
        return self._input_vars

    def invalid_inputs(self):
        """
        Function gets a list of inputs with a null value.
        :return: list of invalid inputs
        """
        return [k for k, v in self._input_vars.items() if not v]

    def update_inputs(self, **kwargs):
        self._input_vars.update(kwargs)

    def _init_querytemplatestring(self):
        """
        This function will initialise the sql template and all child templates
        :return: None
        """

        env = Environment(loader=FileSystemLoader(self.templatepath))

        ast = env.parse(self._querytemplate)

        input_varset = meta.find_undeclared_variables(ast)

        ts = meta.find_referenced_templates(ast)

        for i in ts:
            template_source = env.loader.get_source(env, i)[0]
            _ast = env.parse(template_source)
            var_set = meta.find_undeclared_variables(_ast)
            input_varset.update(var_set)

        # Convert to a dictionary
        self._input_vars = dict.fromkeys(input_varset, None)

    def _render_template(self):

        env = Environment(loader=FileSystemLoader(self.templatepath))

        template = env.from_string(self._querytemplate)

        return template.render(self._input_vars)  # Render using our context values

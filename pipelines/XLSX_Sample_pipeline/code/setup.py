from setuptools import setup, find_packages
setup(
    name = 'XLSX_Sample_pipeline',
    version = '1.0',
    packages = find_packages(include = ('xlsx_sample_pipeline*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.8.19'],
    entry_points = {
'console_scripts' : [
'main = xlsx_sample_pipeline.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)

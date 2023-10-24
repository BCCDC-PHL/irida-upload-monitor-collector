from setuptools import setup, find_namespace_packages


setup(
    name='irida-upload-monitor-collector',
    version='0.1.0-alpha-0',
    packages=find_namespace_packages(),
    entry_points={
        "console_scripts": [
            "irida-upload-monitor-collector = irida_upload_monitor_collector.__main__:main",
        ]
    },
    scripts=[],
    package_data={
    },
    install_requires=[
    ],
    description='Collect Information on IRIDA Uploads',
    url='https://github.com/BCCDC-PHL/irida-upload-monitor-collector',
    author='Dan Fornika',
    author_email='dan.fornika@bccdc.ca',
    include_package_data=True,
    keywords=[],
    zip_safe=False
)

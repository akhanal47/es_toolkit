import setuptools

setuptools.setup(
    name="es_toolkit",
    version="0.1",
    author='Anup Khanal',
    author_email='anup.khanal@coyotes.usd.edu',
    description='Create a Slack Alter using Slack Webhook',
    url='https://github.com/akhanal47/es_toolkit',
    packages=['es_toolkit'],
        install_requires=[
        # Add any dependencies required by your package
        'slack_sdk==3.21.3',
        'python_dotenv==1.0.0',
        'elasticsearch==8.8.0',
        'requests==2.31.0',
        'configparser==5.3.0'
    ]
)
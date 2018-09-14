from setuptools import setup

setup(name='pyclickhouse',
      version='0.4.1',
      description='Minimalist Clickhouse Python driver with an API roughly resembling Python DB API 2.0 specification.',
      url='https://github.com/Immowelt/PyClickhouse',
      download_url = 'https://github.com/Immowelt/PyClickhouse/archive/0.4.1.tar.gz',
      keywords = ['Clickhouse', 'Database', 'Driver'],
      classifiers=[],
      author='Immowelt AG',
      author_email='m.fridental@immowelt.de',
      license='Apache2',
      packages=['pyclickhouse'],
      zip_safe=False)


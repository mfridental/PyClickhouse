from setuptools import setup

REQUIRED=['requests', 'future', 'six', 'ujson']

setup(name='pyclickhouse3',
      version='0.9.13',
      description='Minimalist Clickhouse Python driver with an API roughly resembling Python DB API 2.0 specification.',
      url='https://github.com/mfridental/PyClickhouse',
      download_url='https://github.com/mfridental/PyClickhouse/archive/0.9.13.tar.gz',
      keywords=['Clickhouse', 'Database', 'Driver'],
      classifiers=[],
      author='Maxim Fridental (Maintainer)',
      author_email='maxim@fridental.de',
      license='Apache2',
      packages=['pyclickhouse'],
      install_requires=REQUIRED,
      use_2to3=True,
      test_suite='test',
      zip_safe=False)


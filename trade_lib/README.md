# Packaging Trade Login, Indicator, and Logger python code

**Created this package based on this** 

    https://packaging.python.org/en/latest/tutorials/packaging-projects/

**Use the following commands for packaging, build, and upload purpose**

    pip install fyers-apiv2

    py -m pip install --upgrade build

    py -m build

    pip install ./dist/trade_lib-2.0.tar.gz 

**Generated the package available at following location**

    dist/
    
      example-package-YOUR-USERNAME-HERE-0.0.1-py3-none-any.whl
    
      example-package-YOUR-USERNAME-HERE-0.0.1.tar.gz

**Uploading the distribution archives, crate account pypi website**

    py -m pip install --upgrade twine

    py -m twine upload --repository testpypi dist/*
import importlib.util, os, sys
p = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'bronze', 'ingestao.py'))
print('Testing module at', p)
spec = importlib.util.spec_from_file_location('ingestao_module', p)
mod = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(mod)
except Exception as e:
    print('ERROR importing module:', e)
    sys.exit(2)

print('validar_credenciais_gcp present:', hasattr(mod, 'validar_credenciais_gcp') and callable(mod.validar_credenciais_gcp))
# Ensure function raises when env var missing
orig = os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
try:
    try:
        mod.validar_credenciais_gcp()
        print('ERROR: expected RuntimeError when env var missing')
        sys.exit(3)
    except RuntimeError as e:
        print('RuntimeError raised as expected:', str(e).split('\n')[0])
finally:
    if orig is not None:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = orig
print('Test completed successfully')

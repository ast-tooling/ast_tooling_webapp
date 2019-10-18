"""
WSGI config for astwebapp project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/howto/deployment/wsgi/
"""

import os
import sys

from django.core.wsgi import get_wsgi_application

sys.path.append('var/www/ssnj-devast01/ast_tooling_webapp')
sys.path.append('var/www/ssnj-devast01/ast_tooling_webapp/astwebapp')

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'astwebapp.settings')

application = get_wsgi_application()

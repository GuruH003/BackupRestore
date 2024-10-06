from django.urls import path
from .views import *

urlpatterns = [
    path('ListIndexes/', ViewIndexes.as_view(),name='List-Indexes'),
    path('BackupIndexes/',BackupIndexes.as_view(),name='Backup-Indexes'),
    path('RestoreIndexes/',RestoreIndexes.as_view(),name='Restore-Indexes'),
    path('CreateIndex/', RestoreIndexes.as_view(),name='Create-index')
]
from django.conf.urls import url
from django.contrib import admin
import views

urlpatterns = [
    url(r'^$', views.home, name='index'),
    url(r'^admin/', admin.site.urls),
    url(r'^experiment/([0-9]{2,4})/$', views.experiment),
    url(r'^experiments/$', views.experiment_list, name='experiment_list'),
    url(r'^screencast/$', views.video, name='video'),
]
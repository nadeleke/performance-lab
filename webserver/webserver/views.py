from django.http import HttpResponse
from models import *
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
import simplejson as json
from django.core import serializers
from django.shortcuts import render

def paginate(request, query_set, limit=25):
    paginator = Paginator(query_set, limit)

    page = request.GET.get('page')
    try:
        query_set = paginator.page(page)
    except PageNotAnInteger:
        # If page is not an integer, deliver first page.
        query_set = paginator.page(1)
    except EmptyPage:
        # If page is out of range (e.g. 9999), deliver last page of results.
        query_set = paginator.page(paginator.num_pages)
    return query_set

def experiment_list(request):
    experiments = paginate(request, Experiments.objects.all())
    return render(request, 'experiment_list.html', context={'experiment_list': experiments})

def experiment(request, id):
    avg_dict = {}
    first_factor = None
    for model in [avg_hw_cpu_arch, avg_hw_cpu_mhz, avg_hw_gpu_mhz, avg_hw_num_cpus, avg_hw_page_sz, avg_hw_ram_mhz, avg_hw_ram_sz, avg_sw_address_randomization, avg_sw_autogroup, avg_sw_compiler, avg_sw_drop_caches, avg_sw_env_padding, avg_sw_filesystem, avg_sw_freq_scaling, avg_sw_link_order, avg_sw_opt_flag, avg_sw_swap, avg_sw_sys_time]:
        items = model.objects.filter(experiment_id=id)
        for i in items:
            field = model.__name__.replace('avg_', '')
            # set the default factor displayed
            if not first_factor:
                first_factor = field
            avg_dict.setdefault(field, [])
            avg_dict[field].append({
                'level': getattr(i, field),
                'setup': i.setup_time,
                'collect': i.collect_time,
                'run': i.run_time
            })

    return render(request, 'experiment.html', context={'json': json.dumps(avg_dict), 'factors': avg_dict.keys(), 'first_factor': first_factor, 'experiment_id': id})

def home(request):
    return render(request, 'home.html')
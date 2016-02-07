function experiment_detail() {
  function getValues(factor_data) {
    var values = _.map(factor_data, function (item){
      return item.run;
    });
    return values;
  }

  function getLabels(factor_data) {
    var labels = _.map(factor_data, function (item) {
      return item.level;
    });
    return labels;
  }
  if (_.isEmpty(data)) {
    return;
  }
  var labelMap = {
    'hw_cpu_arch': 'CPU architecture',
    'hw_cpu_mhz': ' CPU clockspeed (MHz)',
    'hw_gpu_mhz': ' GPU clockspeed (MHz)',
    'hw_num_cpus': ' Number of CPU cores',
    'hw_page_sz': ' Page size',
    'hw_ram_mhz': ' RAM frequency (MHz)',
    'hw_ram_sz': ' RAM size',
    'sw_address_randomization': ' Address randomization',
    'sw_autogroup': ' Kernel autogroup',
    'sw_compiler': ' Compiler',
    'sw_drop_caches': ' Drop caches',
    'sw_env_padding': ' Environment padding',
    'sw_filesystem': ' Filesystem',
    'sw_freq_scaling': ' Frequency Scaling',
    'sw_link_order': ' Link order',
    'sw_opt_flag': ' Optimization flag',
    'sw_swap': ' Swap'
  };
  var options = {
    chart: {
      renderTo: 'container',
      defaultSeriesType: 'spline',
      type: data[available_factor].length > 1 ? 'line' : 'bar'
    },
    title: {
      text: labelMap[available_factor]
    },
    xAxis: {
      categories: getLabels(data[available_factor]),
    },
    yAxis: {
        title: {
            text: 'Run time (s)'
        },
    },
    series: [{name: available_factor, data: getValues(data[available_factor])}]
  };
  var defaultOptions = options;
  var chart = new Highcharts.Chart(options);

  $("#list").on('change', function () {
    var selVal = $("#list").val();
    if (selVal == available_factor || selVal == '') {
      options = defaultOptions;
    }
    else {
      options = {
        chart: {
          renderTo: 'container',
          defaultSeriesType: 'spline',
          type: data[available_factor].length > 1 ? 'line' : 'bar'
        },
        yAxis: {
            title: {
                text: 'Run time (s)'
            },
        },
        title: {
          text: labelMap[selVal]
        },
        xAxis: {
          categories: getLabels(data[selVal]),
        },
        series: [{name: selVal, data: getValues(data[selVal])}]
      };
    }
    var chart = new Highcharts.Chart(options);
  });
}

function experiment_list() {

}

function job_list() {

}

mapping = {
  'experiments\/': experiment_list,
  'experiment\/.*\/': experiment_detail,
  'jobs\/': job_list
};

var url_pattern;
for (url_pattern in mapping) {
  action = mapping[url_pattern];
  if ((new RegExp(url_pattern)).test(document.URL)) {
    action();
    break;
  }
}

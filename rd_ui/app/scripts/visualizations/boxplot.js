(function() {
  var module = angular.module('redash.visualization');

  module.config(['VisualizationProvider', function(VisualizationProvider) {
      var renderTemplate =
        '<boxplot-renderer ' +
        'options="visualization.options" query-result="queryResult">' +
        '</boxplot-renderer>';

      var editTemplate = '<boxplot-editor></boxplot-editor>';
      var defaultOptions = {
        boxplotColName: 'boxplot',
        rowNumber: 1,
        targetRowNumber: 1
      };

      VisualizationProvider.registerVisualization({
        type: 'BOXPLOT',
        name: 'boxplot',
        renderTemplate: renderTemplate,
        editorTemplate: editTemplate,
        defaultOptions: defaultOptions
      });
    }
  ]);
  module.directive('boxplotRenderer', function() {
    return {
      restrict: 'E',
      templateUrl: '/views/visualizations/boxplot.html',
      link: function($scope, elm, attrs) {

        
        var loaddata = function(inputdata){
        };

        function iqr(k) {
          return function(d, i) {
            var q1 = d.quartiles[0],
                q3 = d.quartiles[2],
                iqr = (q3 - q1) * k,
                i = -1,
                j = d.length;
            while (d[++i] < q1 - iqr);
            while (d[--j] > q3 + iqr);
            return [i, j];
          };
        };

        $scope.$watch('queryResult && queryResult.getData()', function (data) {
              
          var colName = $scope.visualization.options.colName;
          var margin = {top: 10, bottom: 20},
              height = 500 - margin.top - margin.bottom;

          var min = Infinity,
              max = -Infinity;
          var mydata = [];
          var value = 0;
          var d = [];

          var columns = $scope.queryResult.columnNames;
          var parentWidth = d3.select(elm[0].parentNode).node().getBoundingClientRect().width;
          var xscale = d3.scale.ordinal()
            .domain(columns)
            .rangeBands([0, parentWidth]);

          if (columns.length > 1){
            boxWidth = Math.min(xscale(columns[1]),120.0);
          } else {boxWidth=120.0}
          leftMargin = boxWidth/3.0;

          _.each(columns, function(column, i){
            d = mydata[i] = [];
            _.each(data, function (row) {
              value = row[column];
              d.push(value);
              if (value > max) max = Math.ceil(value);
              if (value < min) min = Math.floor(value);
            });
          });

          var chart = d3.box()
              .whiskers(iqr(1.5))
              .width(boxWidth-2*leftMargin)
              .height(height)
              .domain([min,max]);   
                
          d3.select(elm[0]).selectAll("svg").remove();

          d3.select(elm[0]).selectAll("svg").data(mydata)
            .enter().append("svg")
              .attr("class", "box")
              .attr("width", boxWidth)
              .attr("height", height + margin.bottom + margin.top)
            .append("g")
              .attr("transform", "translate(" + leftMargin + "," + margin.top + ")")
              .call(chart); 

        });
      }
    }
  });

  module.directive('boxplotEditor', function() {
    return {
      restrict: 'E',
      templateUrl: '/views/visualizations/boxplot_editor.html'
    };
  });

})();

<!doctype html>

<html lang="en">
<head>
    <meta charset="utf-8">

    <title>utopia-php/database</title>
    <meta name="description" content="utopia-php/database">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

</head>

<body>

<div class="canvascontainer">
    <canvas id="myChart"></canvas>
</div>

<script>

const colors = [
    {
        line: "rgb(0, 184, 148)",
        fill: "rgba(0, 184, 148, 0.2)"
    },
    {
        line: "rgb(214, 48, 49)",
        fill: "rgba(214, 48, 49, 0.2)"
    },
    {
        line: "rgb(9, 132, 227)",
        fill: "rgba(9, 132, 227, 0.2)"
    },
    {
        line: "rgb(0, 206, 201)",
        fill: "rgba(0, 206, 201, 0.2)"
    },
];

let results = <?php

    $directory = './results';
    $scanned_directory = array_diff(scandir($directory), array('..', '.'));
    // $files = '["' . implode('", "', $scanned_directory) . '"]';

    $results = [];
    foreach ($scanned_directory as $path) {
        $results[] = [
            'name' => $path,
            'data' => json_decode(file_get_contents("{$directory}/{$path}"), true)
        ];
    }
    echo json_encode($results);
?>

console.log(results);

let datasets = [];

for (i=0; i < results.length; i++) {
    datasets[i] = {
        label: results[i].name,
        data: results[i].data[1].results,
        fill: true,
        backgroundColor: colors[i].fill,
        borderColor: colors[i].line,
        pointBackgroundColor: colors[i].line,
        pointBorderColor: '#fff',
        pointHoverBackgroundColor: '#fff',
        pointHoverBorderColor: colors[i].line
    }
}

const data = {
    labels: [
        'created.greater(), genre.equal()',
        'genre.equal(OR)',
        'views.greater()',
        'text.search()',
    ],
    datasets: datasets,
};

const config = {
  type: 'radar',
  data: data,
  options: {
    elements: {
      line: {
        borderWidth: 2
      }
    },
    responsive: true,
    maintainAspectRatio: false
  },
};

var myChart = new Chart(
    document.getElementById('myChart'),
    config
);

</script>

<style>
.canvascontainer {
    width: 75vw;
    height: 75vh;
    margin: auto;
}
</style>

</body>
</html>

<!doctype html>

<html lang="en">
<head>
    <meta charset="utf-8">

    <title>utopia-php/database</title>
    <meta name="description" content="utopia-php/database">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://unpkg.com/vue@next"></script>

</head>

<body>

    <div class="chartcontainer">
        <canvas id="radarchart"></canvas>
    </div>

    <div id="datatables" class="datatables">
        <table v-for="n in 4" :key="n">
            <tr>
                <th colspan="5">{{ queries[n-1] }}</th>
            </tr>
            <tr>
                <th>1 role</th>
                <th>100 roles</th>
                <th>500 roles</th>
                <th>1000 roles</th>
                <th>2000 roles</th>
            </tr>
            <tr v-for="result in results" :key="result.name">
                <td v-for="set in result.data">{{ set.results[n-1].toFixed(4) }}</td>
            </tr>
        </table>
    </div>

<script>

const results = <?php
    $directory = './results';
    $scanned_directory = array_diff(scandir($directory), array('..', '.'));

    $results = [];
    foreach ($scanned_directory as $path) {
        $results[] = [
            'name' => $path,
            'data' => json_decode(file_get_contents("{$directory}/{$path}"), true)
        ];
    }
    echo json_encode($results);
?>

console.log(results)

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

// Radar chart
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

const chartData = {
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
  data: chartData,
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

const myChart = new Chart(
    document.getElementById('radarchart'),
    config
);

// datatables with vue
const datatables = {
    data() {
        return {
        results: results,
        queries: ['created.greater(), genre.equal()', 'genre.equal(OR)', 'views.greater()', 'text.search()']
        }
    }
}

Vue.createApp(datatables).mount('#datatables')

</script>

<style>
table {
    margin: 1em;
}

.chartcontainer {
    width: 75vw;
    height: 65vh;
    margin: auto;
}

.datatables {
    display: flex;
    flex-flow: row wrap;
    padding-left: 2em;
    padding-right: 2em;
    margin-left: auto;
    margin-right: auto;
}
</style>

</body>
</html>

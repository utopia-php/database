<?php

/**
 * @var CLI $cli
 */
global $cli;

use Utopia\CLI\CLI;

$cli
    ->task('merge-coverage')
    ->desc('Merge clover code coverage reports into a single file')
    ->action(function () {
        $files = \glob('coverage/*.xml');

        if (empty($files)) {
            echo "No coverage files found!" . PHP_EOL;
            return;
        }

        $merged = new DOMDocument('1.0', 'UTF-8');
        $merged->appendChild($merged->createElement('coverage'));

        // Loop through all Clover XML files in the coverage directory
        foreach ($files as $file) {
            $report = new DOMDocument();
            $report->load($file);

            $project = $report
                ->getElementsByTagName('project')
                ->item(0);

            $importedProject = $merged->importNode($project, true);

            $merged->documentElement->appendChild($importedProject);
        }

        // Save the merged report as an XML file
        $merged->save('coverage/clover.xml');

        echo 'Merged report generated' . PHP_EOL;
    });

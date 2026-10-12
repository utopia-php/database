<?php

/**
 * Loads every class of the library in a process where ext-swoole is unavailable. A native type naming a Swoole
 * class is resolved when PHP checks an override against its parent, which is a fatal error when the class is missing.
 *
 * Run by SwooleAbsentTest through a subprocess started with -n.
 */

require dirname(__DIR__, 3).'/vendor/autoload.php';

echo 'swoole='.(extension_loaded('swoole') ? '1' : '0').PHP_EOL;

$root = dirname(__DIR__, 3).'/src';
$classes = [];
foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator($root, FilesystemIterator::SKIP_DOTS)) as $file) {
    if ($file instanceof SplFileInfo && $file->getExtension() === 'php') {
        $classes[] = 'Utopia\\'.str_replace('/', '\\', substr($file->getPathname(), strlen($root) + 1, -4));
    }
}
sort($classes);

foreach ($classes as $class) {
    echo 'loading='.$class.PHP_EOL;
    if (! class_exists($class) && ! interface_exists($class) && ! trait_exists($class) && ! enum_exists($class)) {
        echo 'missing='.$class.PHP_EOL;
        exit(1);
    }
}

echo 'loaded='.count($classes).PHP_EOL;

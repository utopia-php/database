<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\CLI\CLI;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Migration\Generator;
use Utopia\DI\Dependency;

final class CLITasksTest extends TestCase
{
    public function testMigrationCommandsUseInjectedDatabaseFactory(): void
    {
        $path = \sys_get_temp_dir().'/database-migrations-'.\bin2hex(\random_bytes(8));
        \mkdir($path);
        $cli = new CLI(args: [
            'bin/cli',
            'migrate:status',
            '--path='.$path,
            '--adapter=memory',
            '--name=testing',
            '--namespace=cli',
            '--sharedTables=0',
        ]);
        $GLOBALS['cli'] = $cli;
        include __DIR__.'/../../bin/tasks/migrate.php';
        unset($GLOBALS['cli']);

        $this->assertSame(
            ['migrate', 'migrate:rollback', 'migrate:status', 'migrate:fresh', 'migrate:generate'],
            \array_keys($cli->getTasks()),
        );

        foreach (['migrate', 'migrate:rollback', 'migrate:status', 'migrate:fresh'] as $task) {
            $this->assertSame(['database'], $cli->getTasks()[$task]->getDependencies());
        }
        $this->assertSame([], $cli->getTasks()['migrate:generate']->getDependencies());

        $received = [];
        $database = null;
        $resource = new Dependency();
        $resource
            ->setName('database')
            ->setCallback(function () use (&$received, &$database): callable {
                return function (string $adapter, string $name, string $namespace, bool $sharedTables) use (&$received, &$database): Database {
                    $received = \func_get_args();
                    $database = (new Database(new Memory(), new Cache(new NoCache())))
                        ->setDatabase($name)
                        ->setNamespace($namespace)
                        ->setSharedTables($sharedTables);
                    $database->create();

                    return $database;
                };
            });
        $cli->setResource($resource);
        $caught = null;
        $cli
            ->error()
            ->inject('error')
            ->action(function (\Throwable $error) use (&$caught): void {
                $caught = $error;
            });
        $cli->run();

        $this->assertNull($caught);
        $this->assertSame(['memory', 'testing', 'cli', false], $received);
        $this->assertInstanceOf(Database::class, $database);
        $this->assertTrue($database->exists('testing', '_migrations'));

        \rmdir($path);
    }

    public function testMainCliIncludesMigrationCommands(): void
    {
        $source = \file_get_contents(__DIR__.'/../../bin/cli.php');
        $this->assertIsString($source);
        $this->assertStringContainsString("include 'tasks/migrate.php';", $source);
        $this->assertStringContainsString('if (! $database->exists()) {', $source);
        $this->assertStringContainsString('$database->create();', $source);
    }

    public function testOperatorSetupUsesDatabaseAuthorization(): void
    {
        $source = \file_get_contents(__DIR__.'/../../bin/tasks/operators.php');
        $this->assertIsString($source);
        $this->assertStringContainsString(
            '$database->getAuthorization()->addRole(Role::any()->toString());',
            $source,
        );
        $this->assertStringNotContainsString('$authorization->', $source);
    }

    /**
     * migrate:generate writes the class under a namespace, so the runner has to
     * find it by more than the filename. Looking up the bare filename finds
     * nothing, and nothing complains -- the migration is skipped and the run
     * reports success having done none of it.
     */
    public function testAGeneratedMigrationIsFoundByTheRunner(): void
    {
        $path = \sys_get_temp_dir().'/database-migrations-'.\bin2hex(\random_bytes(8));
        \mkdir($path);

        $className = 'V20260101000000_AddWidgets';
        \file_put_contents($path.'/'.$className.'.php', (new Generator())->generateEmpty($className));

        $this->includeMigrationTasks();

        $migrations = \loadMigrations($path);

        $this->assertCount(1, $migrations, 'A migration written by migrate:generate has to be picked up by the runner');
        $this->assertSame('20260101000000', $migrations[0]->version());
    }

    public function testAMigrationDeclaredWithoutANamespaceIsStillFound(): void
    {
        $path = \sys_get_temp_dir().'/database-migrations-'.\bin2hex(\random_bytes(8));
        \mkdir($path);

        $className = 'V20260101000001_AddGadgets';
        \file_put_contents($path.'/'.$className.'.php', <<<PHP
        <?php

        use Utopia\Database\Database;
        use Utopia\Database\Migration\Migration;

        class {$className} extends Migration
        {
            public function version(): string
            {
                return '20260101000001';
            }

            public function up(Database \$db): void
            {
            }

            public function down(Database \$db): void
            {
            }
        }
        PHP);

        $this->includeMigrationTasks();

        $migrations = \loadMigrations($path);

        $this->assertCount(1, $migrations);
        $this->assertSame('20260101000001', $migrations[0]->version());
    }

    private function includeMigrationTasks(): void
    {
        if (\function_exists('loadMigrations')) {
            return;
        }

        $GLOBALS['cli'] = new CLI(args: ['bin/cli', 'migrate:status']);
        include __DIR__.'/../../bin/tasks/migrate.php';
        unset($GLOBALS['cli']);
    }
}

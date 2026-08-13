<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\CLI\CLI;

final class CLITasksTest extends TestCase
{
    public function testMigrationCommandsRegister(): void
    {
        $cli = new CLI(args: ['bin/cli', 'migrate']);
        $GLOBALS['cli'] = $cli;
        include __DIR__.'/../../bin/tasks/migrate.php';
        unset($GLOBALS['cli']);

        $this->assertSame(
            ['migrate', 'migrate:rollback', 'migrate:status', 'migrate:fresh', 'migrate:generate'],
            \array_keys($cli->getTasks()),
        );
    }

    public function testMainCliIncludesMigrationCommands(): void
    {
        $source = \file_get_contents(__DIR__.'/../../bin/cli.php');
        $this->assertIsString($source);
        $this->assertStringContainsString("include 'tasks/migrate.php';", $source);
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
}

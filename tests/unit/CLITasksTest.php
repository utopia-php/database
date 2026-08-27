<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;

final class CLITasksTest extends TestCase
{
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

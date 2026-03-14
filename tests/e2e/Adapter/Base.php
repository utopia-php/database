<?php

namespace Tests\E2E\Adapter;

use PHPUnit\Framework\TestCase;
use Tests\E2E\Adapter\Scopes\AggregationTests;
use Tests\E2E\Adapter\Scopes\AttributeTests;
use Tests\E2E\Adapter\Scopes\CollectionTests;
use Tests\E2E\Adapter\Scopes\CustomDocumentTypeTests;
use Tests\E2E\Adapter\Scopes\DocumentTests;
use Tests\E2E\Adapter\Scopes\GeneralTests;
use Tests\E2E\Adapter\Scopes\IndexTests;
use Tests\E2E\Adapter\Scopes\JoinTests;
use Tests\E2E\Adapter\Scopes\ObjectAttributeTests;
use Tests\E2E\Adapter\Scopes\OperatorTests;
use Tests\E2E\Adapter\Scopes\PermissionTests;
use Tests\E2E\Adapter\Scopes\RelationshipTests;
use Tests\E2E\Adapter\Scopes\SchemalessTests;
use Tests\E2E\Adapter\Scopes\SpatialTests;
use Tests\E2E\Adapter\Scopes\VectorTests;
use Utopia\Database\Database;
use Utopia\Database\Hook\RelationshipHandler;
use Utopia\Database\Validator\Authorization;

\ini_set('memory_limit', '2048M');

abstract class Base extends TestCase
{
    use AggregationTests;
    use AttributeTests;
    use CollectionTests;
    use CustomDocumentTypeTests;
    use DocumentTests;
    use GeneralTests;
    use IndexTests;
    use JoinTests;
    use ObjectAttributeTests;
    use OperatorTests;
    use PermissionTests;
    use RelationshipTests;
    use SchemalessTests;
    use SpatialTests;
    use VectorTests;

    protected static string $namespace;

    protected static ?Authorization $authorization = null;

    abstract protected function getDatabase(): Database;

    abstract protected function deleteColumn(string $collection, string $column): bool;

    abstract protected function deleteIndex(string $collection, string $index): bool;

    protected function setUp(): void
    {
        $this->testDatabase = 'utopiaTests_'.static::getTestToken();

        if (is_null(self::$authorization)) {
            self::$authorization = new Authorization();
        }

        self::$authorization->addRole('any');

        $db = $this->getDatabase();
        if ($db->getRelationshipHook() === null) {
            $db->setRelationshipHook(new RelationshipHandler($db));
        }
    }

    protected function tearDown(): void
    {
        self::$authorization->setDefaultStatus(true);

    }

    protected string $testDatabase = 'utopiaTests';

    protected static function getTestToken(): string
    {
        return getenv('TEST_TOKEN') ?: getenv('UNIQUE_TEST_TOKEN') ?: (string) getmypid();
    }
}

<?php

namespace Tests\Unit\Exception;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Exception;
use Utopia\Database\Exception\Authorization;
use Utopia\Database\Exception\Character;
use Utopia\Database\Exception\Conflict;
use Utopia\Database\Exception\Contention;
use Utopia\Database\Exception\Dependency;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Exception\Index;
use Utopia\Database\Exception\Limit;
use Utopia\Database\Exception\Mismatch;
use Utopia\Database\Exception\NotFound;
use Utopia\Database\Exception\Operator;
use Utopia\Database\Exception\Order;
use Utopia\Database\Exception\Query;
use Utopia\Database\Exception\Relationship;
use Utopia\Database\Exception\Restricted;
use Utopia\Database\Exception\Schema;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Exception\Timeout;
use Utopia\Database\Exception\Transaction;
use Utopia\Database\Exception\Truncate;
use Utopia\Database\Exception\Type;
use Utopia\Database\Exception\Unconfirmed;
use Utopia\Database\Exception\Unique;

final class HierarchyTest extends TestCase
{
    /**
     * @return array<string, array{class-string<Exception>, class-string<Exception>}>
     */
    public static function parents(): array
    {
        return [
            'schema' => [Schema::class, Exception::class],
            'structure' => [Structure::class, Schema::class],
            'type' => [Type::class, Schema::class],
            'character' => [Character::class, Schema::class],
            'truncate' => [Truncate::class, Schema::class],
            'index' => [Index::class, Schema::class],
            'dependency' => [Dependency::class, Schema::class],
            'limit' => [Limit::class, Schema::class],
            'relationship' => [Relationship::class, Schema::class],
            'query' => [Query::class, Exception::class],
            'order' => [Order::class, Query::class],
            'operator' => [Operator::class, Query::class],
            'transaction' => [Transaction::class, Exception::class],
            'contention' => [Contention::class, Transaction::class],
            'timeout' => [Timeout::class, Exception::class],
            'unconfirmed' => [Unconfirmed::class, Exception::class],
            'duplicate' => [Duplicate::class, Exception::class],
            'unique' => [Unique::class, Duplicate::class],
            'mismatch' => [Mismatch::class, Duplicate::class],
            'authorization' => [Authorization::class, Exception::class],
            'conflict' => [Conflict::class, Exception::class],
            'not found' => [NotFound::class, Exception::class],
            'restricted' => [Restricted::class, Exception::class],
        ];
    }

    /**
     * @param  class-string<Exception>  $exception
     * @param  class-string<Exception>  $parent
     */
    #[DataProvider('parents')]
    public function testParent(string $exception, string $parent): void
    {
        $this->assertSame($parent, \get_parent_class($exception));
    }

    /**
     * @return array<string, array{class-string<Exception>}>
     */
    public static function neverRetried(): array
    {
        return [
            'timeout' => [Timeout::class],
            'unconfirmed' => [Unconfirmed::class],
        ];
    }

    /**
     * @param  class-string<Exception>  $exception
     */
    #[DataProvider('neverRetried')]
    public function testOutsideTheTransactionSubtree(string $exception): void
    {
        $this->assertFalse(\is_subclass_of($exception, Transaction::class));
    }

    /**
     * @return array<string, array{Exception}>
     */
    public static function outsideSchema(): array
    {
        return [
            'query' => [new Query('Invalid query')],
            'order' => [new Order('Invalid order')],
            'not found' => [new NotFound('Collection not found')],
            'duplicate' => [new Duplicate('Document already exists')],
            'transaction' => [new Transaction('Failed to start transaction')],
        ];
    }

    #[DataProvider('outsideSchema')]
    public function testOtherFailuresAreNotSchemaFailures(Exception $exception): void
    {
        $this->assertNotInstanceOf(Schema::class, $exception);
    }

    public function testACatchOfQueryCatchesOrderAndOperator(): void
    {
        foreach ([new Order('Invalid order', 'name'), new Operator('Invalid operator')] as $failure) {
            try {
                throw $failure;
            } catch (Query $caught) {
                $this->assertSame($failure, $caught);
            }
        }
    }
}

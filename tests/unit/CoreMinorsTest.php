<?php

namespace Tests\Unit;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\Role;

final class CoreMinorsTest extends TestCase
{
    /**
     * @return array<string, array{Throwable}>
     */
    public static function deterministicFailures(): array
    {
        return [
            'authorization' => [new AuthorizationException('denied')],
            'character' => [new CharacterException('bad character')],
            'duplicate' => [new DuplicateException('duplicate')],
            'limit' => [new LimitException('limit')],
            'not found' => [new NotFoundException('missing')],
            'order' => [new OrderException('order')],
            'query' => [new QueryException('query')],
            'relationship' => [new RelationshipException('relationship')],
            'restricted' => [new RestrictedException('restricted')],
            'structure' => [new StructureException('structure')],
            'type' => [new TypeException('type')],
        ];
    }

    #[DataProvider('deterministicFailures')]
    public function testDeterministicFailuresAreNotRetried(Throwable $failure): void
    {
        $writes = 0;
        $database = $this->metadataFailing($failure, $writes);

        $error = $this->attempt(fn (): bool => $database->createAttribute('logs', Attribute::integer(key: 'count')));

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame($failure, $error->getPrevious(), 'The deterministic failure must reach the caller');
        $this->assertSame(1, $writes, 'A deterministic failure must not be retried');
    }

    public function testTransientFailuresAreRetried(): void
    {
        $failure = new RuntimeException('connection reset');
        $writes = 0;
        $database = $this->metadataFailing($failure, $writes);

        $error = $this->attempt(fn (): bool => $database->createAttribute('logs', Attribute::integer(key: 'count')));

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame($failure, $error->getPrevious());
        $this->assertSame(3, $writes, 'An unknown failure must still be retried');
    }

    /**
     * A database with a `logs` collection whose later metadata writes count into $writes and throw $failure.
     */
    private function metadataFailing(Throwable $failure, int &$writes): Database
    {
        $failing = false;
        $database = $this->interceptingMetadataWrites(function () use (&$failing, &$writes, $failure): void {
            if (! $failing) {
                return;
            }

            $writes++;

            throw $failure;
        });
        $this->configure($database);
        $database->createCollection(new Collection(id: 'logs'));
        $failing = true;

        return $database;
    }

    /**
     * A database that runs $intercept before every write of a collection definition.
     *
     * @param  Closure(): void  $intercept
     */
    private function interceptingMetadataWrites(Closure $intercept, ?Adapter $adapter = null): Database
    {
        return new class ($adapter ?? $this->adapter(), new Cache(new None()), $intercept) extends Database {
            /**
             * @param  Closure(): void  $intercept
             */
            public function __construct(Adapter $adapter, Cache $cache, private readonly Closure $intercept)
            {
                parent::__construct($adapter, $cache);
            }

            #[\Override]
            public function updateDocument(string $collection, string $id, Document $document): Document
            {
                if ($collection === self::METADATA) {
                    ($this->intercept)();
                }

                return parent::updateDocument($collection, $id, $document);
            }
        };
    }

    private function adapter(): Adapter
    {
        return new SQLite(new PDO('sqlite::memory:'));
    }

    private function configure(Database $database): void
    {
        $database
            ->setDatabase('core_minors')
            ->setNamespace('core_minors_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
    }

    /**
     * @param  callable(): mixed  $operation
     */
    private function attempt(callable $operation): ?Throwable
    {
        try {
            $operation();
        } catch (Throwable $error) {
            return $error;
        }

        return null;
    }
}

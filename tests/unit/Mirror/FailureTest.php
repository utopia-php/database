<?php

namespace Tests\Unit\Mirror;

use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Event;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Mirror;
use Utopia\Database\Mirror\Failure;

final class FailureTest extends TestCase
{
    public function testASchemaChangeTheDestinationRejectsIsReportedWithItsMethodAndEvent(): void
    {
        $destination = $this->database(new Memory());
        $mirror = new Mirror($this->database(new Memory()), $destination);
        $failures = $this->failures($mirror);
        $destination->createCollection($this->notes());

        $created = $mirror->createCollection($this->notes());

        $failure = $this->only($failures);
        $this->assertSame('notes', $created->getId());
        $this->assertSame('createCollection', $failure->method);
        $this->assertSame(Event::CollectionCreate, $failure->event);
        $this->assertInstanceOf(DuplicateException::class, $failure->error);
    }

    public function testASettingTheDestinationRejectsIsReportedWithoutAnEvent(): void
    {
        $refusal = new RuntimeException('destination unreachable');
        $destination = $this->database(new class ($refusal) extends Memory {
            public function __construct(private readonly RuntimeException $refusal)
            {
                parent::__construct();
            }

            /**
             * @return array<Capability>
             */
            public function capabilities(): array
            {
                return [...parent::capabilities(), Capability::AlterLock];
            }

            public function setLocks(bool $locks): static
            {
                throw $this->refusal;
            }
        });
        $mirror = new Mirror($this->database(new Memory()), $destination);
        $failures = $this->failures($mirror);

        $mirror->setLocks(true);

        $failure = $this->only($failures);
        $this->assertSame('setLocks', $failure->method);
        $this->assertNull($failure->event);
        $this->assertSame($refusal, $failure->error);
    }

    public function testEveryCallbackReceivesTheSameFailure(): void
    {
        $destination = $this->database(new Memory());
        $mirror = new Mirror($this->database(new Memory()), $destination);
        $destination->createCollection($this->notes());
        $first = [];
        $second = [];

        $chained = $mirror
            ->onError(static function (Failure $failure) use (&$first): void {
                $first[] = $failure;
            })
            ->onError(static function (Failure $failure) use (&$second): void {
                $second[] = $failure;
            });
        $mirror->createCollection($this->notes());

        $this->assertSame($mirror, $chained);
        $this->assertCount(1, $first);
        $this->assertSame($first, $second);
    }

    public function testAMirrorWithoutAFailingDestinationReportsNothing(): void
    {
        $mirror = new Mirror($this->database(new Memory()), $this->database(new Memory()));
        $failures = $this->failures($mirror);

        $mirror->createCollection($this->notes());

        $this->assertSame([], $failures->getArrayCopy());
    }

    /**
     * @return \ArrayObject<int, Failure>
     */
    private function failures(Mirror $mirror): \ArrayObject
    {
        /** @var \ArrayObject<int, Failure> $failures */
        $failures = new \ArrayObject();
        $mirror->onError(static function (Failure $failure) use ($failures): void {
            $failures->append($failure);
        });

        return $failures;
    }

    /**
     * @param  \ArrayObject<int, Failure>  $failures
     */
    private function only(\ArrayObject $failures): Failure
    {
        $this->assertCount(1, $failures);
        $failure = $failures[0] ?? null;
        $this->assertInstanceOf(Failure::class, $failure);

        return $failure;
    }

    private function database(Memory $adapter): Database
    {
        $database = (new Database($adapter, new Cache(new None())))
            ->setDatabase('mirror')
            ->setNamespace('failure_'.\uniqid());
        $database->create();

        return $database;
    }

    private function notes(): Collection
    {
        return Collection::create(id: 'notes', attributes: [Attribute::string(key: 'title', size: 64)]);
    }
}

<?php

namespace Tests\Unit\Mirror;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Tests\Unit\Hook\QueriesRecorder;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Event\Domain;
use Utopia\Database\Hook\Lifecycle;
use Utopia\Database\Mirror;
use Utopia\Database\Query;

final class QueriesHookTest extends TestCase
{
    /**
     * @return iterable<string, array{bool}>
     */
    public static function lifecycles(): iterable
    {
        yield 'a queries hook' => [false];
        yield 'a queries hook that is also a lifecycle hook' => [true];
    }

    #[DataProvider('lifecycles')]
    public function testTheMirrorRewritesItsOwnReadsOnce(bool $lifecycle): void
    {
        $hook = self::hook($lifecycle);
        $source = HookFixture::memory();
        HookFixture::seed($source, ['first', 'second']);
        $mirror = new Mirror($source, HookFixture::memory());

        $mirror->addHook($hook);

        $this->assertSame(['first'], self::ids($mirror->find(HookFixture::COLLECTION)));
        $this->assertSame([[Event::DocumentFind, HookFixture::COLLECTION]], $hook->calls);
    }

    public function testNeitherSideRewritesItsReadsWithTheMirrorsHook(): void
    {
        $hook = self::hook(false);
        $source = HookFixture::memory();
        $destination = HookFixture::memory();
        HookFixture::seed($source, ['first', 'second']);
        HookFixture::seed($destination, ['first', 'second']);
        $mirror = new Mirror($source, $destination);

        $mirror->addHook($hook);

        $this->assertSame(['first', 'second'], self::ids($mirror->getSource()->find(HookFixture::COLLECTION)));
        $this->assertSame(['first', 'second'], self::ids($destination->find(HookFixture::COLLECTION)));
        $this->assertSame([], $hook->calls);
    }

    public function testTheMirrorReplicatesAWriteWhileALifecycleQueriesHookRewritesTheSourcesReads(): void
    {
        $hook = self::hook(true);
        $source = HookFixture::memory();
        $destination = HookFixture::memory();
        $mirror = new Mirror($source, $destination);
        $mirror->createUpgrades();
        $source->getAuthorization()->skip(static fn (): Document => $source->createDocument('upgrades', new Document([
            '$id' => HookFixture::COLLECTION,
            'collectionId' => HookFixture::COLLECTION,
            'status' => 'upgraded',
        ])));

        $mirror->addHook($hook);
        $mirror->createDocument(HookFixture::COLLECTION, new Document(['$id' => 'third', 'title' => 'third', 'views' => 3]));

        $this->assertSame([], $hook->calls);
        $this->assertSame('third', $destination->getDocument(HookFixture::COLLECTION, 'third')->getAttribute('title'));
    }

    private static function hook(bool $lifecycle): QueriesRecorder
    {
        $adding = QueriesRecorder::adding(Query::equal('title', ['first']));

        if (! $lifecycle) {
            return new QueriesRecorder($adding);
        }

        return new class ($adding) extends QueriesRecorder implements Lifecycle {
            #[\Override]
            public function handle(Domain $event): void
            {
            }
        };
    }

    /**
     * @param  array<Document>  $documents
     * @return list<string>
     */
    private static function ids(array $documents): array
    {
        return \array_values(\array_map(static fn (Document $document): string => $document->getId(), $documents));
    }
}

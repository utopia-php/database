<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;

class DocumentsValidatorCacheTest extends TestCase
{
    private Document $orders;

    private Document $customers;

    protected function setUp(): void
    {
        $this->orders = new Document([
            '$id' => 'orders',
            'attributes' => [],
            'indexes' => [],
        ]);
        $this->customers = new Document([
            '$id' => 'customers',
            'attributes' => [],
            'indexes' => [],
        ]);
    }

    public function testValidatorIsReusedWithoutJoinedCollections(): void
    {
        $database = new DocumentsValidatorDatabase(new Memory(), new Cache(new None()));

        $this->assertSame(
            $database->documentsValidator($this->orders),
            $database->documentsValidator($this->orders),
        );
    }

    public function testJoinedCollectionsBypassTheCache(): void
    {
        $database = new DocumentsValidatorDatabase(new Memory(), new Cache(new None()));

        $cached = $database->documentsValidator($this->orders);
        $joined = $database->documentsValidator($this->orders, [$this->customers]);

        $this->assertNotSame($cached, $joined);
        $this->assertNotSame($joined, $database->documentsValidator($this->orders, [$this->customers]));
        $this->assertSame($cached, $database->documentsValidator($this->orders));
    }

    public function testMirrorForwardsJoinedCollectionsToTheSource(): void
    {
        $mirror = new DocumentsValidatorMirror(new Database(new Memory(), new Cache(new None())));

        $cached = $mirror->documentsValidator($this->orders);

        $this->assertSame($cached, $mirror->documentsValidator($this->orders));
        $this->assertNotSame($cached, $mirror->documentsValidator($this->orders, [$this->customers]));
    }
}

<?php

namespace Tests\Unit\Indexes;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Index;

final class LegacyJunctionIndexTest extends TestCase
{
    public function testA7xJunctionIndexIsReadUnderItsStoredId(): void
    {
        $index = Index::fromDocument(new Document(['$id' => '_index_tags', 'key' => 'index_tags', 'type' => 'key', 'attributes' => ['tags']]));

        $this->assertSame('_index_tags', $index->key);
    }

    public function testAnyOtherIndexKeepsItsKey(): void
    {
        $this->assertSame('index_tags', Index::fromDocument(new Document(['$id' => 'index_tags', 'key' => 'index_tags', 'type' => 'key', 'attributes' => ['tags']]))->key);
        $this->assertSame('title', Index::fromDocument(new Document(['$id' => 'renamed', 'key' => 'title', 'type' => 'key', 'attributes' => ['tags']]))->key);
        $this->assertSame('_index_books', Index::fromDocument(new Document(['$id' => '_index_books', 'key' => '_index_books', 'type' => 'key', 'attributes' => ['books']]))->key);
    }
}

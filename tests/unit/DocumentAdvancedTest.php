<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\SetType;

class DocumentAdvancedTest extends TestCase
{
    public function testDeepCloneWithNestedDocuments(): void
    {
        $inner = new Document(['$id' => 'inner', 'value' => 'original']);
        $middle = new Document(['$id' => 'middle', 'child' => $inner]);
        $outer = new Document(['$id' => 'outer', 'child' => $middle]);

        $cloned = clone $outer;

        /** @var Document $clonedMiddle */
        $clonedMiddle = $cloned->getAttribute('child');
        /** @var Document $clonedInner */
        $clonedInner = $clonedMiddle->getAttribute('child');

        $clonedInner->setAttribute('value', 'modified');

        $this->assertSame('original', $inner->getAttribute('value'));
        $this->assertSame('modified', $clonedInner->getAttribute('value'));
    }

    public function testDeepCloneWithArrayOfDocuments(): void
    {
        $doc = new Document([
            '$id' => 'parent',
            'items' => [
                new Document(['$id' => 'a', 'val' => 1]),
                new Document(['$id' => 'b', 'val' => 2]),
            ],
        ]);

        $cloned = clone $doc;

        /** @var array<Document> $clonedItems */
        $clonedItems = $cloned->getAttribute('items');
        $clonedItems[0]->setAttribute('val', 99);

        /** @var array<Document> $originalItems */
        $originalItems = $doc->getAttribute('items');
        $this->assertSame(1, $originalItems[0]->getAttribute('val'));
        $this->assertSame(99, $clonedItems[0]->getAttribute('val'));
    }

    public function testFindWithSubjectKey(): void
    {
        $doc = new Document([
            '$id' => 'root',
            'items' => [
                new Document(['$id' => 'item1', 'name' => 'first']),
                new Document(['$id' => 'item2', 'name' => 'second']),
            ],
        ]);

        $found = $doc->find('name', 'second', 'items');
        $this->assertInstanceOf(Document::class, $found);
        $this->assertSame('item2', $found->getId());
    }

    public function testFindReturnsDocumentOnDirectMatch(): void
    {
        $doc = new Document(['$id' => 'test', 'status' => 'active']);

        $result = $doc->find('status', 'active');
        $this->assertInstanceOf(Document::class, $result);
        $this->assertSame('test', $result->getId());
    }

    public function testFindReturnsFalseWhenNotFound(): void
    {
        $doc = new Document([
            '$id' => 'test',
            'items' => [
                new Document(['$id' => 'a', 'name' => 'alpha']),
            ],
        ]);

        $this->assertFalse($doc->find('name', 'nonexistent', 'items'));
    }

    public function testFindReturnsFalseForDirectMismatch(): void
    {
        $doc = new Document(['$id' => 'test', 'status' => 'active']);
        $this->assertFalse($doc->find('status', 'inactive'));
    }

    public function testFindAndReplaceWithSubject(): void
    {
        $doc = new Document([
            '$id' => 'root',
            'items' => [
                new Document(['$id' => 'a', 'name' => 'alpha']),
                new Document(['$id' => 'b', 'name' => 'beta']),
            ],
        ]);

        $result = $doc->findAndReplace('name', 'alpha', new Document(['$id' => 'a', 'name' => 'replaced']), 'items');
        $this->assertTrue($result);

        /** @var array<Document> $items */
        $items = $doc->getAttribute('items');
        $this->assertSame('replaced', $items[0]->getAttribute('name'));
    }

    public function testFindAndReplaceReturnsFalseForMissing(): void
    {
        $doc = new Document([
            '$id' => 'root',
            'items' => [
                new Document(['$id' => 'a', 'name' => 'alpha']),
            ],
        ]);

        $this->assertFalse($doc->findAndReplace('name', 'nonexistent', 'new', 'items'));
    }

    public function testFindAndRemoveWithSubject(): void
    {
        $doc = new Document([
            '$id' => 'root',
            'items' => [
                new Document(['$id' => 'a', 'name' => 'alpha']),
                new Document(['$id' => 'b', 'name' => 'beta']),
                new Document(['$id' => 'c', 'name' => 'gamma']),
            ],
        ]);

        $result = $doc->findAndRemove('name', 'beta', 'items');
        $this->assertTrue($result);

        /** @var array<Document> $items */
        $items = $doc->getAttribute('items');
        $this->assertCount(2, $items);
    }

    public function testFindAndRemoveReturnsFalseForMissing(): void
    {
        $doc = new Document([
            '$id' => 'root',
            'items' => [
                new Document(['$id' => 'a', 'name' => 'alpha']),
            ],
        ]);

        $this->assertFalse($doc->findAndRemove('name', 'nonexistent', 'items'));
    }

    public function testGetArrayCopyWithAllowFilter(): void
    {
        $doc = new Document([
            '$id' => 'test',
            'name' => 'John',
            'email' => 'john@example.com',
            'age' => 30,
        ]);

        $copy = $doc->getArrayCopy(['name', 'email']);

        $this->assertArrayHasKey('name', $copy);
        $this->assertArrayHasKey('email', $copy);
        $this->assertArrayNotHasKey('$id', $copy);
        $this->assertArrayNotHasKey('age', $copy);
    }

    public function testGetArrayCopyWithDisallowFilter(): void
    {
        $doc = new Document([
            '$id' => 'test',
            'name' => 'John',
            'secret' => 'hidden',
            'password' => '12345',
        ]);

        $copy = $doc->getArrayCopy([], ['secret', 'password']);

        $this->assertArrayHasKey('$id', $copy);
        $this->assertArrayHasKey('name', $copy);
        $this->assertArrayNotHasKey('secret', $copy);
        $this->assertArrayNotHasKey('password', $copy);
    }

    public function testGetArrayCopyWithNestedDocuments(): void
    {
        $doc = new Document([
            '$id' => 'parent',
            'child' => new Document(['$id' => 'child', 'value' => 'test']),
        ]);

        $copy = $doc->getArrayCopy();
        $this->assertIsArray($copy['child']);
        $this->assertSame('child', $copy['child']['$id']);
        $this->assertSame('test', $copy['child']['value']);
    }

    public function testGetArrayCopyWithArrayOfDocuments(): void
    {
        $doc = new Document([
            '$id' => 'parent',
            'children' => [
                new Document(['$id' => 'a']),
                new Document(['$id' => 'b']),
            ],
        ]);

        $copy = $doc->getArrayCopy();
        $this->assertIsArray($copy['children']);
        $this->assertCount(2, $copy['children']);
        $this->assertSame('a', $copy['children'][0]['$id']);
        $this->assertSame('b', $copy['children'][1]['$id']);
    }

    public function testIsEmptyOnDifferentStates(): void
    {
        $empty = new Document();
        $this->assertTrue($empty->isEmpty());

        $withId = new Document(['$id' => 'test']);
        $this->assertFalse($withId->isEmpty());

        $withAttribute = new Document(['name' => 'test']);
        $this->assertFalse($withAttribute->isEmpty());
    }

    public function testGetAttributeWithDefaultValue(): void
    {
        $doc = new Document(['$id' => 'test', 'name' => 'John']);

        $this->assertSame('John', $doc->getAttribute('name', 'default'));
        $this->assertSame('default', $doc->getAttribute('missing', 'default'));
        $this->assertNull($doc->getAttribute('missing'));
        $this->assertSame(0, $doc->getAttribute('missing', 0));
        $this->assertSame([], $doc->getAttribute('missing', []));
        $this->assertFalse($doc->getAttribute('missing', false));
    }

    public function testRemoveAttribute(): void
    {
        $doc = new Document([
            '$id' => 'test',
            'name' => 'John',
            'email' => 'john@example.com',
        ]);

        $result = $doc->removeAttribute('name');

        $this->assertInstanceOf(Document::class, $result);
        $this->assertNull($doc->getAttribute('name'));
        $this->assertFalse($doc->isSet('name'));
        $this->assertSame('john@example.com', $doc->getAttribute('email'));
    }

    public function testRemoveAttributeReturnsSelf(): void
    {
        $doc = new Document(['$id' => 'test', 'a' => 1, 'b' => 2]);

        $result = $doc->removeAttribute('a')->removeAttribute('b');

        $this->assertInstanceOf(Document::class, $result);
        $this->assertFalse($doc->isSet('a'));
        $this->assertFalse($doc->isSet('b'));
    }

    public function testSetAttributesBatch(): void
    {
        $doc = new Document(['$id' => 'test']);

        $doc->setAttributes([
            'name' => 'John',
            'email' => 'john@example.com',
            'age' => 25,
        ]);

        $this->assertSame('John', $doc->getAttribute('name'));
        $this->assertSame('john@example.com', $doc->getAttribute('email'));
        $this->assertSame(25, $doc->getAttribute('age'));
    }

    public function testSetAttributesBatchOverwrites(): void
    {
        $doc = new Document(['$id' => 'test', 'name' => 'Old']);

        $doc->setAttributes(['name' => 'New', 'extra' => 'added']);

        $this->assertSame('New', $doc->getAttribute('name'));
        $this->assertSame('added', $doc->getAttribute('extra'));
    }

    public function testSetAttributesBatchReturnsSelf(): void
    {
        $doc = new Document(['$id' => 'test']);
        $result = $doc->setAttributes(['a' => 1]);

        $this->assertSame($doc, $result);
    }

    public function testGetAttributesFiltersInternalKeys(): void
    {
        $doc = new Document([
            '$id' => 'test',
            '$collection' => 'users',
            '$permissions' => ['read("any")'],
            '$createdAt' => '2024-01-01T00:00:00.000+00:00',
            '$updatedAt' => '2024-01-01T00:00:00.000+00:00',
            'name' => 'John',
            'email' => 'john@example.com',
        ]);

        $attrs = $doc->getAttributes();

        $this->assertArrayHasKey('name', $attrs);
        $this->assertArrayHasKey('email', $attrs);
        $this->assertArrayNotHasKey('$id', $attrs);
        $this->assertArrayNotHasKey('$collection', $attrs);
        $this->assertArrayNotHasKey('$permissions', $attrs);
        $this->assertArrayNotHasKey('$createdAt', $attrs);
        $this->assertArrayNotHasKey('$updatedAt', $attrs);
    }

    public function testSetTypeAppend(): void
    {
        $doc = new Document(['$id' => 'test', 'tags' => ['php']]);

        $doc->setAttribute('tags', 'laravel', SetType::Append);

        $this->assertSame(['php', 'laravel'], $doc->getAttribute('tags'));
    }

    public function testSetTypeAppendOnNonArray(): void
    {
        $doc = new Document(['$id' => 'test', 'value' => 'scalar']);

        $doc->setAttribute('value', 'item', SetType::Append);

        $this->assertSame(['item'], $doc->getAttribute('value'));
    }

    public function testSetTypeAppendOnMissing(): void
    {
        $doc = new Document(['$id' => 'test']);

        $doc->setAttribute('newList', 'first', SetType::Append);

        $this->assertSame(['first'], $doc->getAttribute('newList'));
    }

    public function testSetTypePrepend(): void
    {
        $doc = new Document(['$id' => 'test', 'tags' => ['php']]);

        $doc->setAttribute('tags', 'html', SetType::Prepend);

        $this->assertSame(['html', 'php'], $doc->getAttribute('tags'));
    }

    public function testSetTypePrependOnNonArray(): void
    {
        $doc = new Document(['$id' => 'test', 'value' => 'scalar']);

        $doc->setAttribute('value', 'item', SetType::Prepend);

        $this->assertSame(['item'], $doc->getAttribute('value'));
    }

    public function testSetTypePrependOnMissing(): void
    {
        $doc = new Document(['$id' => 'test']);

        $doc->setAttribute('newList', 'first', SetType::Prepend);

        $this->assertSame(['first'], $doc->getAttribute('newList'));
    }

    public function testSetTypeAssign(): void
    {
        $doc = new Document(['$id' => 'test', 'name' => 'old']);

        $doc->setAttribute('name', 'new', SetType::Assign);

        $this->assertSame('new', $doc->getAttribute('name'));
    }

    public function testConstructorAutoConvertsNestedArraysToDocuments(): void
    {
        $doc = new Document([
            '$id' => 'parent',
            'child' => ['$id' => 'child_id', 'name' => 'nested'],
        ]);

        $child = $doc->getAttribute('child');
        $this->assertInstanceOf(Document::class, $child);
        $this->assertSame('child_id', $child->getId());
    }

    public function testConstructorAutoConvertsArrayOfNestedDocuments(): void
    {
        $doc = new Document([
            '$id' => 'parent',
            'children' => [
                ['$id' => 'a', 'name' => 'first'],
                ['$id' => 'b', 'name' => 'second'],
            ],
        ]);

        /** @var array<Document> $children */
        $children = $doc->getAttribute('children');
        $this->assertCount(2, $children);
        $this->assertInstanceOf(Document::class, $children[0]);
        $this->assertInstanceOf(Document::class, $children[1]);
    }

    public function testFindWithArrayValues(): void
    {
        $doc = new Document([
            '$id' => 'root',
            'items' => [
                ['name' => 'alpha', 'score' => 1],
                ['name' => 'beta', 'score' => 2],
            ],
        ]);

        $found = $doc->find('name', 'beta', 'items');
        $this->assertIsArray($found);
        $this->assertSame('beta', $found['name']);
        $this->assertSame(2, $found['score']);
    }

    public function testGetArrayCopyWithEmptyArrayValues(): void
    {
        $doc = new Document([
            '$id' => 'test',
            'empty_list' => [],
            'non_empty' => ['a'],
        ]);

        $copy = $doc->getArrayCopy();
        $this->assertSame([], $copy['empty_list']);
        $this->assertSame(['a'], $copy['non_empty']);
    }
}

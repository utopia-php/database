<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;

final class DocumentTrimTest extends TestCase
{
    private function document(): Document
    {
        return new Document([
            Document::ID => 'parent',
            'name' => 'Parent',
            'secret' => 'hidden',
            'child' => new Document([Document::ID => 'child', 'name' => 'Child', 'secret' => 'nested']),
            'children' => [
                new Document([Document::ID => 'first', 'secret' => 'first']),
                'plain',
            ],
            'tags' => ['a', 'b'],
        ]);
    }

    public function testGetArrayCopyConvertsNestedDocumentsRecursively(): void
    {
        $this->assertSame([
            Document::ID => 'parent',
            'name' => 'Parent',
            'secret' => 'hidden',
            'child' => [Document::ID => 'child', 'name' => 'Child', 'secret' => 'nested'],
            'children' => [
                [Document::ID => 'first', 'secret' => 'first'],
                'plain',
            ],
            'tags' => ['a', 'b'],
        ], $this->document()->getArrayCopy());
    }

    public function testGetArrayCopyConvertsDocumentsNestedInNestedDocuments(): void
    {
        $document = new Document([
            'child' => new Document(['grandchildren' => [new Document([Document::ID => 'grandchild'])]]),
        ]);

        $this->assertSame(['child' => ['grandchildren' => [[Document::ID => 'grandchild']]]], $document->getArrayCopy());
    }

    public function testOnlyKeepsTheGivenTopLevelKeysInDocumentOrder(): void
    {
        $this->assertSame([
            'name' => 'Parent',
            'child' => [Document::ID => 'child', 'name' => 'Child', 'secret' => 'nested'],
        ], $this->document()->only(['child', 'name']));
    }

    public function testOnlyIgnoresKeysTheDocumentLacks(): void
    {
        $this->assertSame(['name' => 'Parent'], $this->document()->only(['name', 'missing']));
    }

    public function testOnlyWithNoKeysKeepsNothing(): void
    {
        $this->assertSame([], $this->document()->only([]));
    }

    public function testExceptDropsTheGivenTopLevelKeysOnly(): void
    {
        $this->assertSame([
            Document::ID => 'parent',
            'name' => 'Parent',
            'child' => [Document::ID => 'child', 'name' => 'Child', 'secret' => 'nested'],
            'children' => [
                [Document::ID => 'first', 'secret' => 'first'],
                'plain',
            ],
        ], $this->document()->except(['secret', 'tags']));
    }

    public function testExceptWithNoKeysEqualsGetArrayCopy(): void
    {
        $document = $this->document();

        $this->assertSame($document->getArrayCopy(), $document->except([]));
    }

    public function testTrimmingLeavesTheDocumentUntouched(): void
    {
        $document = $this->document();

        $only = $document->only(['child']);
        $only['child']['name'] = 'changed';
        $except = $document->except(['name']);
        $except['children'][0]['secret'] = 'changed';

        $this->assertSame('Parent', $document->getAttribute('name'));
        $this->assertSame('Child', $document->getDocument('child')->getAttribute('name'));
        $this->assertSame('first', $document->getDocuments('children')[0]->getAttribute('secret'));
    }
}

<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Cursor;
use Utopia\Query\Method;

/**
 * Consumers validate client queries before they resolve a cursor ID to its document, so the validator takes both
 * forms; an array is no cursor the database reads.
 */
final class CursorDocumentTest extends TestCase
{
    /**
     * @return array<string, array{Query}>
     */
    public static function directions(): array
    {
        return [
            'after' => [Query::cursorAfter(new Document([Document::ID => 'movie1']))],
            'before' => [Query::cursorBefore(new Document([Document::ID => 'movie1']))],
        ];
    }

    #[DataProvider('directions')]
    public function testADocumentIsAccepted(Query $query): void
    {
        $this->assertTrue((new Cursor())->isValid($query));
    }

    public function testADocumentIdIsAccepted(): void
    {
        $validator = new Cursor();

        $this->assertTrue($validator->isValid(Query::cursorAfter('movie1')));
        $this->assertTrue($validator->isValid(Query::cursorBefore('movie1')));
    }

    public function testAnArrayIsRejected(): void
    {
        $validator = new Cursor();

        $this->assertFalse($validator->isValid(new Query(Method::CursorAfter, values: [[Document::ID => 'movie1']])));
        $this->assertStringContainsString('Invalid cursor', $validator->getDescription());
        $this->assertFalse($validator->isValid(new Query(Method::CursorBefore, values: [[Document::ID => 'movie1']])));
    }
}

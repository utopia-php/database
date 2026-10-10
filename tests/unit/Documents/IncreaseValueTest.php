<?php

namespace Tests\Unit\Documents;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Event\HookFixture;
use Utopia\Database\Database;

final class IncreaseValueTest extends TestCase
{
    /**
     * @return iterable<string, array{int|float|string}>
     */
    public static function invalidChanges(): iterable
    {
        yield 'zero' => [0];
        yield 'negative integer' => [-1];
        yield 'zero float' => [0.0];
        yield 'negative float' => [-0.5];
        yield 'zero string' => ['0'];
        yield 'negative integer string' => ['-4'];
        yield 'negative decimal string' => ['-1.5'];
        yield 'non-numeric string' => ['many'];
    }

    #[DataProvider('invalidChanges')]
    public function testIncreaseRefusesAChangeThatIsNotAPositiveNumber(int|float|string $change): void
    {
        $database = $this->database();

        $this->assertRefused(fn () => $database->increaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views', $change));
        $this->assertSame(1, $database->getDocument(HookFixture::COLLECTION, 'first')->getAttribute('views'));
    }

    #[DataProvider('invalidChanges')]
    public function testDecreaseRefusesAChangeThatIsNotAPositiveNumber(int|float|string $change): void
    {
        $database = $this->database();

        $this->assertRefused(fn () => $database->decreaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views', $change));
        $this->assertSame(1, $database->getDocument(HookFixture::COLLECTION, 'first')->getAttribute('views'));
    }

    public function testTheRefusalIsAnInvalidArgumentAs7xThrew(): void
    {
        $database = $this->database();

        $this->expectException(InvalidArgumentException::class);

        $database->increaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views', 0);
    }

    public function testAPositiveChangeIsApplied(): void
    {
        $database = $this->database();

        $this->assertSame(6, $database->increaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views', '5')->getAttribute('views'));
        $this->assertSame(4, $database->decreaseDocumentAttribute(HookFixture::COLLECTION, 'first', 'views', 2)->getAttribute('views'));
    }

    private function assertRefused(callable $operation): void
    {
        try {
            $operation();
            $this->fail('A change that is not a positive number was accepted');
        } catch (InvalidArgumentException $error) {
            $this->assertSame('Value must be numeric and greater than 0', $error->getMessage());
        }
    }

    private function database(): Database
    {
        $database = HookFixture::memory();
        HookFixture::seed($database, ['first']);

        return $database;
    }
}

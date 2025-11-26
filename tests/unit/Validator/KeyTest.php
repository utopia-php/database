<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Validator\Key;

class KeyTest extends TestCase
{
    /**
     * @var Key
     */
    protected ?Key $object = null;

    public function setUp(): void
    {
        $this->object = new Key();
    }

    public function tearDown(): void
    {
    }

    public function testValues(): void
    {
        // Must be strings
        $this->assertSame(false, $this->object->isValid(false));
        $this->assertSame(false, $this->object->isValid(null));
        $this->assertSame(false, $this->object->isValid(['value']));
        $this->assertSame(false, $this->object->isValid(0));
        $this->assertSame(false, $this->object->isValid(1.5));
        $this->assertSame(true, $this->object->isValid('asdas7as9as'));
        $this->assertSame(true, $this->object->isValid('5f058a8925807'));

        // Don't allow empty string
        $this->assertSame(false, $this->object->isValid(''));
        $this->assertSame(true, $this->object->isValid('0'));
        $this->assertSame(true, $this->object->isValid('null'));

        // No leading special chars
        $this->assertSame(false, $this->object->isValid('_asdasdasdas'));
        $this->assertSame(false, $this->object->isValid('.as5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('-as5dasdasdas'));

        // Special chars allowed: underscore, period, hyphen
        $this->assertSame(true, $this->object->isValid('as5dadasdas_'));
        $this->assertSame(true, $this->object->isValid('as_5dasdasdas'));
        $this->assertSame(true, $this->object->isValid('as5dasdasdas.'));
        $this->assertSame(true, $this->object->isValid('as.5dasdasdas'));
        $this->assertSame(true, $this->object->isValid('as5dasdasdas-'));
        $this->assertSame(true, $this->object->isValid('as-5dasdasdas'));

        $this->assertSame(false, $this->object->isValid('dasda asdasd'));
        $this->assertSame(false, $this->object->isValid('asd"asd6sdas'));
        $this->assertSame(false, $this->object->isValid('asd\'as0asdas'));
        $this->assertSame(false, $this->object->isValid('as!5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as@5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as#5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as$5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as%5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as^5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as&5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as*5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as(5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as)5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as+5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as=5dasdasdas'));

        // At most 36 chars
        $this->assertSame(true, $this->object->isValid(str_repeat('a', 36)));
        $this->assertSame(false, $this->object->isValid(str_repeat('a', 256)));

        // Internal keys
        $validator = new Key(true);
        $this->assertSame(true, $validator->isValid('appwrite'));
        $this->assertSame(true, $validator->isValid('appwrite_'));
        $this->assertSame(false, $validator->isValid('_appwrite'));
        $this->assertSame(false, $validator->isValid('_'));

        $this->assertSame(true, $validator->isValid('$id'));
        $this->assertSame(true, $validator->isValid('$createdAt'));
        $this->assertSame(true, $validator->isValid('$updatedAt'));

        $this->assertSame(false, $validator->isValid('$appwrite'));
        $this->assertSame(false, $validator->isValid('$permissions'));
        $this->assertSame(false, $validator->isValid('$'));
    }
}

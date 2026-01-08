<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Validator\Label;

class LabelTest extends TestCase
{
    /**
     * @var Label
     */
    protected ?Label $object = null;

    public function setUp(): void
    {
        $this->object = new Label();
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

        // No special chars
        $this->assertSame(false, $this->object->isValid('_asdasdasdas'));
        $this->assertSame(false, $this->object->isValid('.as5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('-as5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as5dadasdas_'));
        $this->assertSame(false, $this->object->isValid('as_5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as5dasdasdas.'));
        $this->assertSame(false, $this->object->isValid('as.5dasdasdas'));
        $this->assertSame(false, $this->object->isValid('as5dasdasdas-'));
        $this->assertSame(false, $this->object->isValid('as-5dasdasdas'));
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

        // At most 255 chars
        $this->assertSame(true, $this->object->isValid(str_repeat('a', 36)));
        $this->assertSame(false, $this->object->isValid(str_repeat('a', 256)));
    }
}

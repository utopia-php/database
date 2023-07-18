<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Validator\Label;
use PHPUnit\Framework\TestCase;

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
        $this->assertEquals(false, $this->object->isValid(false));
        $this->assertEquals(false, $this->object->isValid(null));
        $this->assertEquals(false, $this->object->isValid(['value']));
        $this->assertEquals(false, $this->object->isValid(0));
        $this->assertEquals(false, $this->object->isValid(1.5));
        $this->assertEquals(true, $this->object->isValid('asdas7as9as'));
        $this->assertEquals(true, $this->object->isValid('5f058a8925807'));

        // No special chars
        $this->assertEquals(false, $this->object->isValid('_asdasdasdas'));
        $this->assertEquals(false, $this->object->isValid('.as5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('-as5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as5dadasdas_'));
        $this->assertEquals(false, $this->object->isValid('as_5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as5dasdasdas.'));
        $this->assertEquals(false, $this->object->isValid('as.5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as5dasdasdas-'));
        $this->assertEquals(false, $this->object->isValid('as-5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('dasda asdasd'));
        $this->assertEquals(false, $this->object->isValid('asd"asd6sdas'));
        $this->assertEquals(false, $this->object->isValid('asd\'as0asdas'));
        $this->assertEquals(false, $this->object->isValid('as!5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as@5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as#5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as$5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as%5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as^5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as&5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as*5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as(5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as)5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as+5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid('as=5dasdasdas'));

        // At most 36 chars
        $this->assertEquals(true, $this->object->isValid('socialAccountForYoutubeSubscribersss'));
        $this->assertEquals(false, $this->object->isValid('socialAccountForYoutubeSubscriberssss'));
        $this->assertEquals(true, $this->object->isValid('5f058a89258075f058a89258075f058t9214'));
        $this->assertEquals(false, $this->object->isValid('5f058a89258075f058a89258075f058tx9214'));
    }
}

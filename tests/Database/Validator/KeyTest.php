<?php

namespace Utopia\Tests\Validator;

use Utopia\Database\Validator\Key;
use PHPUnit\Framework\TestCase;

class KeyTest extends TestCase
{
    /**
     * @var Key
     */
    protected $object = null;

    public function setUp(): void
    {
        $this->object = new Key();
    }

    public function tearDown(): void
    {
    }

    public function testValues()
    {
        $this->assertEquals($this->object->isValid('dasda asdasd'), false);
        $this->assertEquals($this->object->isValid('asdasdasdas'), true);
        $this->assertEquals($this->object->isValid('_asdasdasdas'), false);
        $this->assertEquals($this->object->isValid('asd"asdasdas'), false);
        $this->assertEquals($this->object->isValid('asd\'asdasdas'), false);
        $this->assertEquals($this->object->isValid('as$$5dasdasdas'), false);
        $this->assertEquals($this->object->isValid(false), false);
        $this->assertEquals($this->object->isValid(null), false);
        $this->assertEquals($this->object->isValid('socialAccountForYoutubeSubscribers'), false);
        $this->assertEquals($this->object->isValid('socialAccountForYoutubeSubscriber'), false);
        $this->assertEquals($this->object->isValid('socialAccountForYoutubeSubscribe'), true);
        $this->assertEquals($this->object->isValid('socialAccountForYoutubeSubscrib'), true);

        $this->assertEquals(true, $this->object->isValid('5f058a8925807'));
        $this->assertEquals(true, $this->object->isValid('5f058a89258075f058a89258075f058t'));
        $this->assertEquals(false, $this->object->isValid('5f058a89258075f058a89258075f058tx'));
    }
}

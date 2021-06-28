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
        $this->assertEquals(false, $this->object->isValid('dasda asdasd'));
        $this->assertEquals(true, $this->object->isValid('asdasdasdas'));
        $this->assertEquals(false, $this->object->isValid('_asdasdasdas'));
        $this->assertEquals(false, $this->object->isValid('asd"asdasdas'));
        $this->assertEquals(false, $this->object->isValid('asd\'asdasdas'));
        $this->assertEquals(false, $this->object->isValid('as$$5dasdasdas'));
        $this->assertEquals(false, $this->object->isValid(false));
        $this->assertEquals(false, $this->object->isValid(null));
        $this->assertEquals(false, $this->object->isValid('socialAccountForYoutubeSubscribers'));
        $this->assertEquals(false, $this->object->isValid('socialAccountForYoutubeSubscriber'));
        $this->assertEquals(true, $this->object->isValid('socialAccountForYoutubeSubscribe'));
        $this->assertEquals(true, $this->object->isValid('socialAccountForYoutubeSubscrib'));

        $this->assertEquals(true, $this->object->isValid('5f058a8925807'));
        $this->assertEquals(true, $this->object->isValid('5f058a89258075f058a89258075f058t'));
        $this->assertEquals(false, $this->object->isValid('5f058a89258075f058a89258075f058tx'));
    }
}

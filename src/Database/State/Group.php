<?php

namespace Utopia\Database\State;

/**
 * The open scopes shared by the {@see Value}s that decide together whether a write by a coroutine cut off from the
 * overrides, because a coroutine between it and their owners has returned, stays local to that coroutine: it does
 * while any override of one of them is open.
 *
 * @internal
 */
final class Group
{
    /**
     * Every open scope of the group's values: the overrides and the local writes of such coroutines.
     */
    public int $open = 0;

    /**
     * The overrides open on the group's values, without the local writes.
     */
    public int $overrides = 0;
}

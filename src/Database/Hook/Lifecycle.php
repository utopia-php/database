<?php

namespace Utopia\Database\Hook;

use Utopia\Database\Event\Domain;
use Utopia\Query\Hook;

/**
 * Lifecycle hook for fire-and-forget side effects on database events.
 *
 * Implementations receive one typed event per database event (document CRUD, collection changes, etc.), a
 * final subclass of {@see Domain} per {@see \Utopia\Database\Event} case, and can respond with side effects
 * (auditing, logging, analytics, event dispatch). The event is built only when a registered hook handles it.
 *
 * Lifecycle hooks differ from {@see Decorator} hooks in two key ways:
 *
 * 1. **Return value**: Lifecycle hooks return void and cannot modify the document or
 *    influence the operation result. Decorators return a Document back into the pipeline.
 *
 * 2. **Error handling**: For document reads and writes, the document purges they cause
 *    and index creation, a lifecycle hook's exception reaches the caller and the
 *    remaining hooks do not run. For every other event it is swallowed and the remaining
 *    hooks still run. An \Error always reaches the caller. Decorator exceptions always
 *    propagate to the caller.
 *
 * Use Decorators when you need to transform documents before they reach the caller.
 */
interface Lifecycle extends Hook
{
    public function handle(Domain $event): void;
}

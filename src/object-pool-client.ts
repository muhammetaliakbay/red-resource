import type { RedisClient } from "./redis";
import { v4 as uuidV4 } from "uuid"
import { Observable, share } from "rxjs"

export class ObjectPoolClient {
    constructor(
        readonly redis: RedisClient,
        readonly pool: string,
    ) {
    }

    private subscriberRedis: RedisClient = null
    readonly $hasQueued: Observable<void> = new Observable<void>(
        subscriber => {
            if (this.subscriberRedis == null) {
                this.subscriberRedis = this.redis.duplicate()
            }

            const onMessage = (channel: string, message: string) => {
                subscriber.next()
            }
            this.subscriberRedis.on('message', onMessage)

            const onError = (err: Error) => {
                subscriber.error(err)
            }
            this.subscriberRedis.on('error', onError)
            
            this.subscriberRedis.subscribe(this.channelHasQueued).catch(
                err => subscriber.error(err)
            )

            return () => {
                this.subscriberRedis.off('message', onMessage)
                this.subscriberRedis.off('error', onError)
                this.subscriberRedis.unsubscribe(this.channelHasQueued)
                this.subscriberRedis.disconnect()
            }
        }
    ).pipe(
        share(),
    )

    readonly channelHasQueued = `${this.pool}:queued` as const
    readonly keyAllObjects = `${this.pool}:all` as const
    readonly keyObjectQueue = `${this.pool}:queue` as const
    readonly keyQueuedObjects = `${this.pool}:queued` as const
    readonly keyClaimedObjects = `${this.pool}:claimed` as const
    readonly keyDelayedObjectQueue = `${this.pool}:delayed-queue` as const
    private partialKeyObjectSession = `${this.pool}:session:` as const
    keyObjectSession(object: string): `${string}:session:${typeof object}` {
        return `${this.partialKeyObjectSession}${object}` as const
    }
    private partialKeyObjectDelay = `${this.pool}:delay:` as const
    keyObjectDelay(object: string): `${string}:delay:${typeof object}` {
        return `${this.partialKeyObjectDelay}${object}` as const
    }
    private partialKeyObjectTags = `${this.pool}:tags:` as const
    keyObjectTags(object: string): `${string}:tags:${typeof object}` {
        return `${this.partialKeyObjectTags}${object}` as const
    }
    private partialKeyTaggedQueue = `${this.pool}:tagged-queue:` as const
    pairTagValue(tag: string, value: string): `${typeof tag}:${typeof value}` {
        return `${tag}:${value}` as const
    }
    keyTaggedQueue(tag: string, value: string): `${string}:tagged-queue:${typeof tag}:${typeof value}` {
        return `${this.partialKeyTaggedQueue}${this.pairTagValue(tag, value)}` as const
    }
    private tmpKeyNewObjects = `${this.pool}:new` as const

    async getAllObjects(): Promise<string[]> {
        return this.redis.smembers(
            this.keyAllObjects,
        )
    }

    async queueTagged(tags: Record<string,string>, objects: string[], delaySeconds: number = 0): Promise<string[]> {
        if (objects.length === 0) {
            return []
        }

        const keysTaggedQueue = Object.entries(tags).map(
            ([tag, value]) => this.keyTaggedQueue(tag, value),
        )

        const tagParameters = Object.entries(tags).flatMap(
            ([tag, value]) => [tag, value] as const,
        )

        const queuedObjects: string[] = await this.redis.eval(
            `
                local tmpKeyNewObjects = KEYS[1]
                local keyAllObjects = KEYS[2]
                local tagCount = tonumber(ARGV[1])
                local tagParametersIndex = 5
                local objectsIndex = tagParametersIndex + (tagCount * 2)
                redis.call('SADD', tmpKeyNewObjects, unpack(ARGV, objectsIndex))
                local newObjects = redis.call('SDIFF', tmpKeyNewObjects, keyAllObjects)
                redis.call('DEL', tmpKeyNewObjects)

                if newObjects[1] == nil then
                    return {}
                end

                redis.call('SADD', keyAllObjects, unpack(newObjects))

                local delaySeconds = tonumber(ARGV[3])
                if delaySeconds > 0 then
                    local keyDelayedObjectQueue = KEYS[5]
                    local partialKeyObjectDelay = ARGV[4]
                    redis.call('RPUSH', keyDelayedObjectQueue, unpack(newObjects))
                    for i, newObject in ipairs(newObjects) do
                        redis.call('SETEX', partialKeyObjectDelay..newObject, delaySeconds, '')
                    end 
                else
                    local keyObjectQueue = KEYS[3]
                    local keyQueuedObjects = KEYS[4]
                    redis.call('SADD', keyQueuedObjects, unpack(newObjects))
                    redis.call('RPUSH', keyObjectQueue, unpack(newObjects))
                end

                if tagCount > 0 then
                    if delaySeconds <= 0 then
                        local keysTaggedQueueIndex = 6
                        for i=0,(tagCount-1) do
                            local keyTaggedQueue = KEYS[keysTaggedQueueIndex + i]
                            redis.call('RPUSH', keyTaggedQueue, unpack(newObjects))
                        end
                    end

                    local partialKeyObjectTags = ARGV[2]
                    for i, newObject in ipairs(newObjects) do
                        redis.call('HSET', partialKeyObjectTags..newObject, unpack(ARGV, tagParametersIndex, objectsIndex - 1))
                    end
                end

                return newObjects
            `,

            5 + keysTaggedQueue.length,

            this.tmpKeyNewObjects,
            this.keyAllObjects,
            this.keyObjectQueue,
            this.keyQueuedObjects,
            this.keyDelayedObjectQueue,
            ...keysTaggedQueue,

            keysTaggedQueue.length,
            this.partialKeyObjectTags,
            delaySeconds,
            this.partialKeyObjectDelay,
            ...tagParameters,
            ...objects,
        )

        if (queuedObjects.length > 0) {
            await this.redis.publish(this.channelHasQueued, '')
        }

        return queuedObjects
    }

    async queue(...objects: string[]): Promise<string[]> {
        return this.queueTagged({}, objects)
    }

    async claim(maxCount: number, expirationSeconds: number, tag?: string): Promise<{session: string, objects: string[]}> {
        const session = uuidV4()
        if (maxCount === 0) {
            return {
                session,
                objects: [],
            }
        }
        const objects = await this.redis.eval(
            `
                local keyObjectQueue = KEYS[1]
                local partialKeyObjectTags = ARGV[6]
                local partialKeyTaggedQueue = ARGV[7]
                local maxCount = tonumber(ARGV[1])
                local tag = ARGV[5]
                local objects

                if tag == '' or maxCount < 2 then
                    objects = redis.call('LPOP', keyObjectQueue, maxCount)
                    if objects == false then
                        return {}
                    end
                else
                    local firstObjects = redis.call('LPOP', keyObjectQueue, 1)
                    if firstObjects == false then
                        return {}
                    end
                    local firstObject = firstObjects[1]
                    local value = redis.call('HGET', partialKeyObjectTags..firstObject, tag)
                    if value == false then
                        objects = firstObjects
                    else
                        local pairTagValue = tag .. ':' .. value
                        local keyTaggedQueue = partialKeyTaggedQueue .. pairTagValue
                        redis.call('LREM', keyTaggedQueue, 1, firstObject)
                        local restObjects = redis.call('LPOP', keyTaggedQueue, maxCount - 1)
                        if restObjects == false then
                            objects = { firstObject }
                        else
                            for _, restObject in ipairs(restObjects) do
                                redis.call('LREM', keyObjectQueue, 1, restObject)
                            end
                            objects = { firstObject, unpack(restObjects) }
                        end
                    end
                end

                local keyQueuedObjects = KEYS[2]
                redis.call('SREM', keyQueuedObjects, unpack(objects))
                
                local partialKeyObjectSession = ARGV[2]
                local session = ARGV[3]
                local expirationSeconds = ARGV[4]

                for _,object in ipairs(objects) do
                    local keyObjectSession = partialKeyObjectSession..object
                    redis.call('SETEX', keyObjectSession, expirationSeconds, session)

                    local tags_values = redis.call('HGETALL', partialKeyObjectTags..object)
                    for i=1,#tags_values,2 do
                        local tag = tags_values[i]
                        local value = tags_values[i + 1]
                        local pairTagValue = tag .. ':' .. value
                        local keyTaggedQueue = partialKeyTaggedQueue .. pairTagValue
                        redis.call('LREM', keyTaggedQueue, 1, object)
                        if redis.call('LLEN', keyTaggedQueue) == 0 then
                            redis.call('DEL', keyTaggedQueue)
                        end
                    end
                end

                local keyClaimedObjects = KEYS[3]
                redis.call('RPUSH', keyClaimedObjects, unpack(objects))

                return objects
            `,

            3,

            this.keyObjectQueue,
            this.keyQueuedObjects,
            this.keyClaimedObjects,

            maxCount,
            this.partialKeyObjectSession,
            session,
            expirationSeconds,
            tag ?? '',
            this.partialKeyObjectTags,
            this.partialKeyTaggedQueue,
        )
        return {
            session,
            objects,
        }
    }

    async extend(object: string | string[], session: string, expirationSeconds: number): Promise<boolean> {
        const objects = Array.isArray(object) ? object : [object]
        const keysObjectSession = objects.map(
            object => this.keyObjectSession(object),
        )
        const result = await this.redis.eval(
            `
                local keysObjectSessionIndex = 2
                local session = ARGV[1]
                local currentSessions = redis.call('MGET', unpack(KEYS, keysObjectSessionIndex))
                for _, currentSession in ipairs(currentSessions) do
                    if currentSession ~= session then
                        return 0
                    end
                end

                local expirationSeconds = ARGV[2]
                for keyObjectSessionIndex=keysObjectSessionIndex,#KEYS do
                    local keyObjectSession = KEYS[keyObjectSessionIndex]
                    redis.call('SETEX', keyObjectSession, expirationSeconds, session)
                end

                local keyClaimedObjects = KEYS[1]
                local objectsIndex = 3
                for i=objectsIndex,#ARGV do
                    local object = ARGV[i]
                    redis.call('LREM', keyClaimedObjects, 1, object)
                end
                redis.call('RPUSH', keyClaimedObjects, unpack(ARGV, objectsIndex))

                return 1
            `,

            1 + keysObjectSession.length,

            this.keyClaimedObjects,
            ...keysObjectSession,

            session,
            expirationSeconds,
            ...objects,
        )

        return result === 1
    }

    async release(object: string | string[], session: string): Promise<boolean> {
        const objects = Array.isArray(object) ? object : [object]
        const keysObjectSession = objects.map(
            object => this.keyObjectSession(object),
        )
        const result = await this.redis.eval(
            `
                local keysObjectSessionIndex = 3
                local session = ARGV[1]
                local currentSessions = redis.call('MGET', unpack(KEYS, keysObjectSessionIndex))
                for _, currentSession in ipairs(currentSessions) do
                    if currentSession ~= session then
                        return 0
                    end
                end

                redis.call('DEL', unpack(KEYS, keysObjectSessionIndex))

                local keyAllObjects = KEYS[1]
                local keyClaimedObjects = KEYS[2]
                local objectsIndex = 3
                for i=objectsIndex,#ARGV do
                    local object = ARGV[i]
                    redis.call('SREM', keyAllObjects, object)
                    redis.call('LREM', keyClaimedObjects, 1, object)
                    
                    local partialKeyObjectTags = ARGV[2]
                    redis.call('DEL', partialKeyObjectTags..object)
                end

                return 1
            `,

            2 + keysObjectSession.length,

            this.keyAllObjects,
            this.keyClaimedObjects,
            ...keysObjectSession,

            session,
            this.partialKeyObjectTags,
            ...objects,
        )

        return result === 1
    }

    async requeue(object: string | string[], session: string, delaySeconds: number = 0): Promise<boolean> {
        const objects = Array.isArray(object) ? object : [object]
        const keysObjectSession = objects.map(
            object => this.keyObjectSession(object),
        )
        const result = await this.redis.eval(
            `
                local keysObjectSessionIndex = 5
                local session = ARGV[1]
                local currentSessions = redis.call('MGET', unpack(KEYS, keysObjectSessionIndex))
                for _, currentSession in ipairs(currentSessions) do
                    if currentSession ~= session then
                        return 0
                    end
                end

                redis.call('DEL', unpack(KEYS, keysObjectSessionIndex))

                local objectsIndex = 6
                local delaySeconds = tonumber(ARGV[4])
                if delaySeconds > 0 then
                    local keyDelayedObjectQueue = KEYS[4]
                    local partialKeyObjectDelay = ARGV[5]
                    redis.call('RPUSH', keyDelayedObjectQueue, unpack(ARGV, objectsIndex))
                    for i=objectsIndex,#ARGV do
                        local object = ARGV[i]
                        redis.call('SETEX', partialKeyObjectDelay..object, delaySeconds, '')
                    end 
                else
                    local keyQueuedObjects = KEYS[1]
                    local keyObjectQueue = KEYS[2]
                    redis.call('SADD', keyQueuedObjects, unpack(ARGV, objectsIndex))
                    redis.call('RPUSH', keyObjectQueue, unpack(ARGV, objectsIndex))
                end

                local keyClaimedObjects = KEYS[3]
                local partialKeyObjectTags = ARGV[2]
                local partialKeyTaggedQueue = ARGV[3]
                for i=objectsIndex,#ARGV do
                    local object = ARGV[i]
                    redis.call('LREM', keyClaimedObjects, 1, object)

                    if delaySeconds <= 0 then
                        local tags_values = redis.call('HGETALL', partialKeyObjectTags..object)
                        for i=1,#tags_values,2 do
                            local tag = tags_values[i]
                            local value = tags_values[i + 1]
                            local pairTagValue = tag .. ':' .. value
                            local keyTaggedQueue = partialKeyTaggedQueue .. pairTagValue
                            redis.call('RPUSH', keyTaggedQueue, object)
                        end
                    end
                end

                return 1
            `,

            4 + keysObjectSession.length,

            this.keyQueuedObjects,
            this.keyObjectQueue,
            this.keyClaimedObjects,
            this.keyDelayedObjectQueue,
            ...keysObjectSession,

            session,
            this.partialKeyObjectTags,
            this.partialKeyTaggedQueue,
            delaySeconds,
            this.partialKeyObjectDelay,
            ...objects,
        )
        const isRequeued = result === 1

        if (isRequeued) {
            await this.redis.publish(this.channelHasQueued, '')
        }

        return isRequeued
    }

    async cleanExpired(): Promise<string[]> {
        const requeuedObjects: string[] = await this.redis.eval(
            `
                local keyClaimedObjects = KEYS[1]
                local claimedObjectCount = redis.call('LLEN', keyClaimedObjects)

                local partialKeyObjectSession = ARGV[1]

                local total = 0

                for i=0,(claimedObjectCount - 1) do
                    local object = redis.call('LINDEX', keyClaimedObjects, i)
                    local keyObjectSession = partialKeyObjectSession..object
                    local objectSessionExists = redis.call('EXISTS', keyObjectSession)
                    
                    if objectSessionExists == 1 then
                        break
                    end
                    total = total + 1
                end

                if total == 0 then
                    return {}
                end
                
                local requeuedObjects = redis.call('LPOP', keyClaimedObjects, total)

                local keyQueuedObjects = KEYS[2]
                local keyObjectQueue = KEYS[3]
                redis.call('SADD', keyQueuedObjects, unpack(requeuedObjects))
                redis.call('RPUSH', keyObjectQueue, unpack(requeuedObjects))

                local partialKeyObjectTags = ARGV[3]
                local partialKeyTaggedQueue = ARGV[4]

                for _, object in ipairs(requeuedObjects) do
                    local tags_values = redis.call('HGETALL', partialKeyObjectTags..object)
                    for i=1,#tags_values,2 do
                        local tag = tags_values[i]
                        local value = tags_values[i + 1]
                        local pairTagValue = tag .. ':' .. value
                        local keyTaggedQueue = partialKeyTaggedQueue .. pairTagValue
                        redis.call('RPUSH', keyTaggedQueue, object)
                    end
                end

                return requeuedObjects
            `,

            3,

            this.keyClaimedObjects,
            this.keyQueuedObjects,
            this.keyObjectQueue,

            this.partialKeyObjectSession,
            this.partialKeyObjectTags,
            this.partialKeyTaggedQueue,
        )

        if (requeuedObjects.length > 0) {
            await this.redis.publish(this.channelHasQueued, '')
        }

        return requeuedObjects
    }

    async cleanDelayed(): Promise<string[]> {
        const queuedObjects: string[] = await this.redis.eval(
            `
                local keyDelayedObjectQueue = KEYS[1]
                local delayedObjectCount = redis.call('LLEN', keyDelayedObjectQueue)

                local partialKeyObjectDelay = ARGV[1]

                local total = 0

                for i=0,(delayedObjectCount - 1) do
                    local object = redis.call('LINDEX', keyDelayedObjectQueue, i)
                    local keyObjectDelay = partialKeyObjectDelay..object
                    local objectDelayExists = redis.call('EXISTS', keyObjectDelay)
                    
                    if objectDelayExists == 1 then
                        break
                    end
                    total = total + 1
                end

                if total == 0 then
                    return {}
                end
                
                local queuedObjects = redis.call('LPOP', keyDelayedObjectQueue, total)

                local keyQueuedObjects = KEYS[2]
                local keyObjectQueue = KEYS[3]
                redis.call('SADD', keyQueuedObjects, unpack(queuedObjects))
                redis.call('RPUSH', keyObjectQueue, unpack(queuedObjects))

                local partialKeyObjectTags = ARGV[3]
                local partialKeyTaggedQueue = ARGV[4]

                for _, object in ipairs(queuedObjects) do
                    local tags_values = redis.call('HGETALL', partialKeyObjectTags..object)
                    for i=1,#tags_values,2 do
                        local tag = tags_values[i]
                        local value = tags_values[i + 1]
                        local pairTagValue = tag .. ':' .. value
                        local keyTaggedQueue = partialKeyTaggedQueue .. pairTagValue
                        redis.call('RPUSH', keyTaggedQueue, object)
                    end
                end

                return queuedObjects
            `,

            3,

            this.keyDelayedObjectQueue,
            this.keyQueuedObjects,
            this.keyObjectQueue,

            this.partialKeyObjectDelay,
            this.partialKeyObjectTags,
            this.partialKeyTaggedQueue,
        )

        if (queuedObjects.length > 0) {
            await this.redis.publish(this.channelHasQueued, '')
        }

        return queuedObjects
    }

    async clean(): Promise<string[]> {
        const requeuedObjects = await this.cleanExpired()
        const queuedObjects = await this.cleanDelayed()
        return [
            ...new Set(
                [
                    ...requeuedObjects,
                    ...queuedObjects,
                ],
            ),
        ]
    }
}

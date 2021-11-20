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
    private partialKeyObjectSession = `${this.pool}:session:` as const
    keyObjectSession(object: string): `${string}:session:${typeof object}` {
        return `${this.partialKeyObjectSession}${object}` as const
    }
    private tmpKeyNewObjects = `${this.pool}:new` as const

    async queue(...objects: string[]): Promise<string[]> {
        if (objects.length === 0) {
            return []
        }
        const queuedObjects: string[] = await this.redis.eval(
            `
                local tmpKeyNewObjects = KEYS[1]
                local keyAllObjects = KEYS[2]
                redis.call('SADD', tmpKeyNewObjects, unpack(ARGV))
                local newObjects = redis.call('SDIFF', tmpKeyNewObjects, keyAllObjects)
                redis.call('DEL', tmpKeyNewObjects)

                if newObjects[1] == nil then
                    return {}
                end

                local keyObjectQueue = KEYS[3]
                local keyQueuedObjects = KEYS[4]
                redis.call('SADD', keyAllObjects, unpack(newObjects))
                redis.call('SADD', keyQueuedObjects, unpack(newObjects))
                redis.call('RPUSH', keyObjectQueue, unpack(newObjects))

                return newObjects
            `,

            4,

            this.tmpKeyNewObjects,
            this.keyAllObjects,
            this.keyObjectQueue,
            this.keyQueuedObjects,

            ...objects,
        )

        if (queuedObjects.length > 0) {
            await this.redis.publish(this.channelHasQueued, '')
        }

        return queuedObjects
    }

    async claim(maxCount: number, expirationSeconds: number): Promise<{session: string, objects: string[]}> {
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
                local maxCount = ARGV[1]
                local objects = redis.call('LPOP', keyObjectQueue, maxCount)
                if objects == false then
                    return {}
                end

                local keyQueuedObjects = KEYS[2]
                redis.call('SREM', keyQueuedObjects, unpack(objects))
                
                local partialKeyObjectSession = ARGV[2]
                local session = ARGV[3]
                local expirationSeconds = ARGV[4]

                for i,object in ipairs(objects) do
                    local keyObjectSession = partialKeyObjectSession..object
                    redis.call('SETEX', keyObjectSession, expirationSeconds, session)
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
        )
        return {
            session,
            objects,
        }
    }

    async extend(object: string, session: string, expirationSeconds: number): Promise<boolean> {
        const result = await this.redis.eval(
            `
                local keyObjectSession = KEYS[1]
                local session = ARGV[1]
                local currentSession = redis.call('GET', keyObjectSession)
                if currentSession ~= session then
                    return 0
                end

                local expirationSeconds = ARGV[2]
                redis.call('SETEX', keyObjectSession, expirationSeconds, session)

                local keyClaimedObjects = KEYS[2]
                local object = ARGV[3]
                redis.call('LREM', keyClaimedObjects, 1, object)
                redis.call('RPUSH', keyClaimedObjects, object)

                return 1
            `,

            2,

            this.keyObjectSession(object),
            this.keyClaimedObjects,

            session,
            expirationSeconds,
            object,
        )

        return result === 1
    }

    async release(object: string, session: string): Promise<boolean> {
        const result = await this.redis.eval(
            `
                local keyObjectSession = KEYS[1]
                local session = ARGV[1]
                local currentSession = redis.call('GET', keyObjectSession)
                if currentSession ~= session then
                    return 0
                end

                redis.call('DEL', keyObjectSession)

                local keyAllObjects = KEYS[2]
                local object = ARGV[2]
                redis.call('SREM', keyAllObjects, object)

                local keyClaimedObjects = KEYS[3]
                redis.call('LREM', keyClaimedObjects, 1, object)

                return 1
            `,

            3,

            this.keyObjectSession(object),
            this.keyAllObjects,
            this.keyClaimedObjects,

            session,
            object,
        )

        return result === 1
    }

    async requeue(object: string, session: string): Promise<boolean> {
        const result = await this.redis.eval(
            `
                local keyObjectSession = KEYS[1]
                local session = ARGV[1]
                local currentSession = redis.call('GET', keyObjectSession)
                if currentSession ~= session then
                    return 0
                end

                redis.call('DEL', keyObjectSession)
                
                local keyQueuedObjects = KEYS[2]
                local keyObjectQueue = KEYS[3]
                local object = ARGV[2]
                redis.call('SADD', keyQueuedObjects, object)
                redis.call('RPUSH', keyObjectQueue, object)

                local keyClaimedObjects = KEYS[4]
                redis.call('LREM', keyClaimedObjects, 1, object)

                return 1
            `,

            4,

            this.keyObjectSession(object),
            this.keyQueuedObjects,
            this.keyObjectQueue,
            this.keyClaimedObjects,

            session,
            object,
        )
        const isRequeued = result === 1

        if (isRequeued) {
            await this.redis.publish(this.channelHasQueued, '')
        }

        return isRequeued
    }

    async clean(): Promise<string[]> {
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

                return requeuedObjects
            `,

            3,

            this.keyClaimedObjects,
            this.keyQueuedObjects,
            this.keyObjectQueue,

            this.partialKeyObjectSession,
        )

        if (requeuedObjects.length > 0) {
            await this.redis.publish(this.channelHasQueued, '')
        }

        return requeuedObjects
    }
}
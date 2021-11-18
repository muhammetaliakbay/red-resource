import type { RedisClient } from "./redis";
import { v4 as uuidV4 } from "uuid"

const EXPIRATION_SECONDS = 60

export class ObjectPool {
    constructor(
        readonly redis: RedisClient,
        readonly pool: string,
    ) {
    }

    readonly keyAllObjects = `${this.pool}:all` as const
    readonly keyObjectQueue = `${this.pool}:queue` as const
    readonly keyQueuedObjects = `${this.pool}:queued` as const
    readonly keyClaimedObjects = `${this.pool}:claimed` as const
    private partialKeyObjectSession = `${this.pool}:session:` as const
    keyObjectSession(object: string): `${string}:session:${typeof object}` {
        return `${this.partialKeyObjectSession}:${object}` as const
    }
    private tmpKeyNewObjects = `${this.pool}:new` as const

    async queue(...objects: string[]) {
        return await this.redis.eval(
            `
                require 'redis'
                
                local tmpKeyNewObjects = KEYS[0]
                local keyAllObjects = KEYS[1]
                redis.call('SADD', tmpKeyNewObjects, unpack(ARGS))
                local newObjects = redis.call('SDIFF', tmpKeyNewObjects, keyAllObjects)
                redis.call('REM', tmpKeyNewObjects)

                local keyObjectQueue = KEYS[2]
                local keyQueuedObjects = KEYS[3]
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
    }

    async claim(maxCount: number) {
        const session = uuidV4()
        return await this.redis.eval(
            `
                require 'redis'
                
                local keyObjectQueue = KEYS[0]
                local maxCount = ARGS[0]
                local objects = redis.call('LPOP', keyObjectQueue, maxCount)
                if objects = nil then
                    return nil
                end
                local count = table.getN(objects)

                local keyQueuedObjects = KEYS[1]
                redis.call('SREM', keyQueuedObjects, unpack(objects))
                
                local partialKeyObjectSession = ARGS[1]
                local session = ARGS[2]
                local expirationSeconds = ARGS[3]

                for i=0,(count - 1) do
                    local object = objects[i]
                    local keyObjectSession = partialKeyObjectSession..object
                    redis.call('SETEX', keyObjectSession, expirationSeconds, session)
                end

                local keyClaimedObjects = KEY[3]
                redis.call('RPUSH', keyClaimedObjects, objects)

                return objects
            `,

            3,

            this.keyObjectQueue,
            this.keyQueuedObjects,
            this.keyClaimedObjects,

            maxCount,
            this.partialKeyObjectSession,
            session,
            EXPIRATION_SECONDS,
        )
    }

    async extend(object: string, session: string): Promise<boolean> {
        const result = await this.redis.eval(
            `
                require 'redis'
                
                local keyObjectSession = KEYS[0]
                local session = ARGS[0]
                local currentSession = redis.call('GET', keyObjectSession)
                if currentSession ~= session then
                    return 0
                end

                local expirationSeconds = ARGS[1]
                redis.call('SETEX', keyObjectSession, expirationSeconds, session)

                local keyClaimedObjects = KEYS[1]
                redis.call('LREM', keyClaimedObjects, 1, object)
                redis.call('RPUSH', keyClaimedObjects, object)

                return 1
            `,

            2,

            this.keyObjectSession(object),
            this.keyClaimedObjects,

            session,
            EXPIRATION_SECONDS,
        )

        return result === 1
    }

    async release(object: string, session: string): Promise<boolean> {
        const result = await this.redis.eval(
            `
                require 'redis'
                
                local keyObjectSession = KEYS[0]
                local session = ARGS[0]
                local currentSession = redis.call('GET', keyObjectSession)
                if currentSession ~= session then
                    return 0
                end

                redis.call('REM', keyObjectSession)

                local keyAllObjects = KEYS[1]
                local object = ARGS[1]
                redis.call('SREM', keyAllObjects, object)

                local keyClaimedObjects = KEYS[2]
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
                require 'redis'
                
                local keyObjectSession = KEYS[0]
                local session = ARGS[0]
                local currentSession = redis.call('GET', keyObjectSession)
                if currentSession ~= session then
                    return 0
                end

                redis.call('REM', keyObjectSession)
                
                local keyQueuedObjects = KEYS[1]
                local keyObjectQueue = KEYS[2]
                local object = ARGS[1]
                redis.call('SADD', keyQueuedObjects, object)
                redis.call('RPUSH', keyObjectQueue, object)

                local keyClaimedObjects = KEYS[2]
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

        return result === 1
    }

    async clean(): Promise<void> {
        return await this.redis.eval(
            `
                require 'redis'

                local keyClaimedObjects = KEYS[0]
                local claimedObjectCount = redis.call('LLEN', keyClaimedObjects)

                local partialKeyObjectSession = ARGS[0]

                local total = 0

                for i=0,(claimedObjectCount - 1) do
                    local object = redis.call('LINDEX', keyClaimedObjects, i)
                    local keyObjectSession = partialKeyObjectSession..object
                    
                    if keyObjectSession = nil then
                        break
                    end
                    total = total + 1
                end
                
                local requeuedObjects = redis.call('LPOP', keyClaimedObjects, total)

                local keyObjectQueue = KEYS[1]
                local keyQueuedObjects = KEYS[2]
                redis.call('SADD', keyQueuedObjects, unpack(requeuedObjects))
                redis.call('RPUSH', keyObjectQueue, unpack(requeuedObjects))

                return requeuedObjects
            `,

            1,

            this.keyClaimedObjects,
            this.keyQueuedObjects,
            this.keyObjectQueue,

            this.partialKeyObjectSession,
        )
    }
}

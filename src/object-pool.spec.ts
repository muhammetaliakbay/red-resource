import { v4 as uuidV4 } from "uuid"
import * as Redis from "ioredis"
import { RedisClient, ObjectPool } from "."
import { expect } from "chai"

const REDIS_HOST = process.env.REDIS_HOST || 'localhost'
const REDIS_PORT = parseInt(process.env.REDIS_PORT, 10) || 6379

describe('ObjectPool', () => {
    let redis: RedisClient
    let pool: string
    let objectPool: ObjectPool

    beforeEach(
        () => {
            redis = new Redis({
                host: 'localhost',
                port: 6379,
            })
            pool = uuidV4()
            objectPool = new ObjectPool(
                redis,
                pool,
            )
        }
    )
    afterEach(
        () => {
            redis.disconnect()
        }
    )

    async function checkRedis({
        all,
        queue,
        claims,
    }: {
        all: string[] | Set<string>,
        queue: string[] | Set<string>,
        claims: string[] | Set<string>,
    }) {
        const allObjects = await redis.smembers(objectPool.keyAllObjects)
        const queuedObjects = await redis.smembers(objectPool.keyQueuedObjects)
        const objectQueue = await redis.lrange(objectPool.keyObjectQueue, 0, -1)
        const claimedObjects = await redis.lrange(objectPool.keyClaimedObjects, 0, -1)
        
        if (!(all instanceof Set)) {
            all = new Set<string>(all)
        }
        const queued = queue instanceof Set ? queue : new Set(queue)

        expect(allObjects).have.members([...all])
        expect(allObjects).have.length(all.size)

        expect(queuedObjects).have.members([...queued])
        expect(queuedObjects).have.length(queued.size)

        if (!(queue instanceof Set)) {
            expect(objectQueue).to.deep.equal(queue)
        }
        if (claims instanceof Set) {
            expect(claimedObjects).have.members([...claims])
            expect(claimedObjects).have.length(claims.size)
        } else {
            expect(claimedObjects).to.deep.equal(claims)
        }
    }
    
    describe('queue method', () => {
        it('should queue the object', async () => {
            const object = uuidV4()
    
            const result = await objectPool.queue(object)
    
            expect(result).to.deep.equal([object])

            await checkRedis({
                all: [object],
                queue: [object],
                claims: [],
            })
        })

        it('should queue multiple objects', async () => {
            const objects = new Set<string>()
            for(let i = 0; i < 10; i ++) {
                objects.add(uuidV4())
            }
    
            const result = await objectPool.queue(...objects)
    
            expect(result).have.members([...objects])
            expect(result).have.length(objects.size)

            await checkRedis({
                all: objects,
                queue: objects,
                claims: [],
            })
        })

        it('should queue repeating objects', async () => {
            const objects = new Set<string>()
            const repeatingObjects = new Set<string>()
            for(let i = 0; i < 10; i ++) {
                const object = uuidV4()
                objects.add(object)
                if (i % 3 === 0) {
                    repeatingObjects.add(object)
                }
            }
    
            const result = await objectPool.queue(...objects, ...repeatingObjects)
    
            expect(result).have.members([...objects])
            expect(result).have.length(objects.size)

            await checkRedis({
                all: objects,
                queue: objects,
                claims: [],
            })
        })
    
        it('should do nothing for empty input', async () => {
            const result = await objectPool.queue()
    
            expect(result).to.deep.equal([])

            await checkRedis({
                all: [],
                queue: [],
                claims: [],
            })
        })

        it('should not queue an existing object', async () => {
            const object = uuidV4()
    
            const resultA = await objectPool.queue(object)
            const resultB = await objectPool.queue(object)

            expect(resultA).to.deep.equal([object])
            expect(resultB).to.deep.equal([])

            await checkRedis({
                all: [object],
                queue: [object],
                claims: [],
            })
        })
    })
})

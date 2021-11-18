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
                host: REDIS_HOST,
                port: REDIS_PORT,
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
        objectSessions = {},
    }: {
        all: string[] | Set<string>,
        queue: string[] | Set<string>,
        claims: string[] | Set<string>,
        objectSessions?: {
            [object: string]: string | null,
        },
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

        for(const [object, session] of Object.entries(objectSessions)) {
            const objectSession = await redis.get(
                objectPool.keyObjectSession(object)
            )
            expect(objectSession).to.equal(session)
        }
    }

    async function forceExpire(object: string, session: string) {
        const expiredSession = await redis.getdel(
            objectPool.keyObjectSession(object)
        )
        expect(expiredSession).to.equal(session)
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
                objectSessions: {
                    [object]: null,
                },
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
                objectSessions: Object.fromEntries(
                    [...objects].map(object => [object, null])
                ),
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
                objectSessions: Object.fromEntries(
                    [...objects].map(object => [object, null])
                ),
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
                objectSessions: {
                    [object]: null,
                },
            })
        })
    })

    describe('claim method', () => {
        it('should claim the object', async () => {
            const object = uuidV4()

            await objectPool.queue(object)
    
            const {
                session,
                objects: claimedObjects,
            } = await objectPool.claim(1, 60)
    
            expect(typeof session).to.equal('string')
            expect(claimedObjects).to.deep.equal([object])

            await checkRedis({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: session,
                },
            })
        })

        it('should not claim anything from empty queue', async () => {
            const {
                session,
                objects: claimedObjects,
            } = await objectPool.claim(1, 60)
    
            expect(typeof session).to.equal('string')
            expect(claimedObjects).to.deep.equal([])

            await checkRedis({
                all: [],
                queue: [],
                claims: [],
            })
        })

        it('should not claim already claimed object', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session: sessionA,
                objects: claimedObjectsA,
            } = await objectPool.claim(1, 60)

            expect(claimedObjectsA).to.deep.equal([object])

            const {
                objects: claimedObjectsB,
            } = await objectPool.claim(1, 60)
    
            expect(claimedObjectsB).to.deep.equal([])

            await checkRedis({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: sessionA,
                },
            })
        })

        it('should not claim expired object', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session: sessionA,
                objects: claimedObjectsA,
            } = await objectPool.claim(1, 60)

            expect(claimedObjectsA).to.deep.equal([object])

            await forceExpire(object, sessionA)

            const {
                objects: claimedObjectsB,
            } = await objectPool.claim(1, 60)
    
            expect(claimedObjectsB).to.deep.equal([])

            await checkRedis({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: null,
                },
            })
        })

        it('should claim cleaned object', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session: sessionA,
                objects: claimedObjectsA,
            } = await objectPool.claim(1, 60)

            expect(claimedObjectsA).to.deep.equal([object])

            await forceExpire(object, sessionA)

            await objectPool.clean()

            await checkRedis({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })

            const {
                session: sessionB,
                objects: claimedObjectsB,
            } = await objectPool.claim(1, 60)
    
            expect(claimedObjectsB).to.deep.equal([object])

            await checkRedis({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: sessionB,
                },
            })
        })
    })
})

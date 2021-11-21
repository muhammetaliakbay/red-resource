import { DynamicModule, Module, OnApplicationBootstrap, OnApplicationShutdown } from "@nestjs/common"
import { DiscoveryModule } from "@nestjs/core";
import { Subscription } from "rxjs";
import { RedisClient } from "./redis";
import { ObjectPool } from "./object-pool";
import { ObjectPoolClient } from "./object-pool-client";
import { ObjectPoolRegistry } from "./object-pool.registry";
import { getObjectPoolToken } from "./object-pool.token";
import { ObjectProcessorExplorer } from "./processor.explorer";
import { REDIS_TOKEN } from "./redis.token";

@Module({
    imports: [
    ],
    providers: [
    ],
})
export class ObjectPoolModule implements OnApplicationBootstrap, OnApplicationShutdown {
    constructor(readonly poolRegistry: ObjectPoolRegistry) {
    }
    
    private cleanSubscription: Subscription
    onApplicationBootstrap() {
        this.cleanSubscription = this.poolRegistry.$clean.subscribe()
    }

    onApplicationShutdown() {
        this.cleanSubscription.unsubscribe()
    }

    static forRoot({
        redis,
    }: {
        redis: RedisClient,
    }): DynamicModule {
        return {
            global: true,
            module: ObjectPoolModule,
            imports: [
                DiscoveryModule,
            ],
            providers: [
                ObjectProcessorExplorer,
                ObjectPoolRegistry,
                {
                    provide: REDIS_TOKEN,
                    useValue: redis,
                },
            ],
            exports: [
                ObjectPoolRegistry,
                REDIS_TOKEN,
            ],
        };
    }

    static register(
        ...pools: string[]
    ): DynamicModule {

        return {
            module: ObjectPoolModule,
            imports: [
            ],
            providers: pools.map(
                pool => ({
                    provide: getObjectPoolToken(pool),
                    inject: [ ObjectPoolRegistry, REDIS_TOKEN ],
                    useFactory:
                        (
                            registry: ObjectPoolRegistry,
                            redis: RedisClient,
                        ) => {
                            const objectPool = new ObjectPool(
                                new ObjectPoolClient(
                                    redis,
                                    pool,
                                ),
                            )
                            registry.add(objectPool)
                            return objectPool
                        }
                }),
            ),
            exports: pools.map(
                pool => getObjectPoolToken(pool),
            ),
        };
    }
}

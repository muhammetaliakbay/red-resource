import { Logger, Module, OnModuleInit } from "@nestjs/common";
import { ObjectPool, ObjectPoolClient, ObjectPoolModule, ObjectPoolRegistry } from "../src"
import { SubModule } from "./sub-module";
import * as Redis from "ioredis"
import { ScheduleModule } from "@nestjs/schedule";

@Module({
    imports: [
        ObjectPoolModule.forRoot({
            redis: new Redis(),
        }),
        SubModule,
        ScheduleModule.forRoot(),
    ],
})
export class DevModule implements OnModuleInit {
    private readonly logger = new Logger('DevModule');

    constructor(
        readonly poolRegistry: ObjectPoolRegistry,
    ) {
    }

    onModuleInit() {
        this.poolRegistry.add(
            new ObjectPool(
                new ObjectPoolClient(
                    new Redis(),
                    "test",
                )
            )
        )
        this.logger.log('Registered test pool')
    }
}

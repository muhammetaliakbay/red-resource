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
export class DevModule {
    constructor(
        readonly poolRegistry: ObjectPoolRegistry,
    ) {
    }
}

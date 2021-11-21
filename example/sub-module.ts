import { Module } from "@nestjs/common";
import { ScheduleModule } from "@nestjs/schedule";
import { ObjectPoolModule } from "../src"
import { Generator } from "./generator";
import { Processor } from "./processor";

@Module({
    imports: [
        ObjectPoolModule.register(
            'test',
        ),
        ScheduleModule,
    ],
    controllers: [
        Processor,
        Generator,
    ]
})
export class SubModule {
}

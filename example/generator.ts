import { Controller, Logger } from "@nestjs/common";
import { ObjectPool, InjectObjectPool } from "../src";
import { Interval } from "@nestjs/schedule"

@Controller()
export class Generator {
    private readonly logger = new Logger('Processor');

    constructor(
        @InjectObjectPool('test')
        readonly pool: ObjectPool,
    ) {
    }

    @Interval(1_000)
    async generate() {
        const object = `${new Date().toLocaleString()}`
        this.logger.log(`Queuing an object to be processed: ${object}`)
        await this.pool.queue(object)
    }
}

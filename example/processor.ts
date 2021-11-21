import { Controller, Logger } from "@nestjs/common";
import { ObjectClaim, ObjectProcessor, Claim, PoolObjects } from "../src";

@Controller()
export class Processor {
    private readonly logger = new Logger('Processor');

    @ObjectProcessor({
        pool: 'test',
        maxClaimCount: 10,
        tag: 'test-tag',
        maxObjectPerClaim: 10,
    })
    async process(
        @PoolObjects objects: string[],
        @ObjectClaim claim: Claim,
    ) {
        this.logger.log(`Claimed object(s) to process, in between 3-8 seconds: ${objects.join(', ')}`)
        await new Promise(resolve => setTimeout(resolve, (Math.random() * 5_000) + 3_000))
        this.logger.log(`Releasing object(s) after processing: ${objects.join(', ')}`)
        await claim.release()
    }
}

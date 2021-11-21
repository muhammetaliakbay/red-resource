import { v4 as uuidV4 } from "uuid";

const tokenHead = uuidV4()
export function getObjectPoolToken(pool: string): string {
    return `${tokenHead}:pool:${pool}`
}

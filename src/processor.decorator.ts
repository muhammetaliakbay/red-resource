import { Claim } from "./object-pool"
import { Inject } from "@nestjs/common";
import { getObjectPoolToken } from "./object-pool.token";

export const OBJECT_PROCESSOR_OPTIONS = Symbol('OBJECT_PROCESSOR_OPTIONS')
export const OBJECT_PROCESSOR_PARAMETERS = Symbol('OBJECT_PROCESSOR_PARAMETERS')

export enum ParameterType {
    Claim = 'claim',
    Object = 'object',
    Objects = 'objects',
}

export type ObjectProcessorOptions = {
    pool: string
    maxClaimCount: number
    queue?: {
        tags?: Record<string, string>,
        objects: string[],
    },
} & ({} | {
    tag: string,
    maxObjectPerClaim?: number,
})

export type ObjectProcessorParameters = {index: number, type: ParameterType}[]

export function ObjectProcessor(
    options: ObjectProcessorOptions,
): MethodDecorator {
    return function (target: any, propertyKey: string) {
        Reflect.defineMetadata(OBJECT_PROCESSOR_OPTIONS, options, target, propertyKey)
    }
}

function Parameter(type: ParameterType): ParameterDecorator {
    return function (target: any, propertyKey: string, parameterIndex: number) {
        let parameters: ObjectProcessorParameters = Reflect.getMetadata(OBJECT_PROCESSOR_PARAMETERS, target, propertyKey)
        if (parameters == null) {
            parameters = []
            Reflect.defineMetadata(OBJECT_PROCESSOR_PARAMETERS, parameters, target, propertyKey)
        }
        parameters.push({
            index: parameterIndex,
            type,
        })
    }
}

export const PoolObject: ParameterDecorator = Parameter(ParameterType.Object)
export const PoolObjects: ParameterDecorator = Parameter(ParameterType.Objects)
export const ObjectClaim: ParameterDecorator = Parameter(ParameterType.Claim)

export function getOptions(target: any, propertyKey: string): ObjectProcessorOptions | undefined {
    return Reflect.getMetadata(OBJECT_PROCESSOR_OPTIONS, target, propertyKey)
}

export function getParameters(target: any, propertyKey: string): ObjectProcessorParameters | undefined {
    return Reflect.getMetadata(OBJECT_PROCESSOR_PARAMETERS, target, propertyKey)
}

export function setArgs(args: any[], parameters: ObjectProcessorParameters, claim: Claim) {
    for(const {index, type} of parameters) {
        switch(type) {
            case ParameterType.Claim:
                args[index] = claim
                break;
            case ParameterType.Objects:
                args[index] = claim.objects
                break;
            case ParameterType.Object:
                args[index] = claim.objects[0]
                break;
        }
    }
}

export function InjectObjectPool(pool: string): ParameterDecorator {
    return Inject(
        getObjectPoolToken(pool),
    )
}

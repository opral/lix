/** The closed-storage binding owns one provider close, including setup failures. */
export async function withConversionProvider<T>(
 provider: {close():Promise<void>},
 convert: ()=>Promise<T>,
):Promise<T> {
 let failed=false;
 try {return await convert();}
 catch(error) {failed=true;throw error;}
 finally {try {await provider.close();}catch(error){if(!failed)throw error;}}
}

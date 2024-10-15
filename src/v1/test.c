/*

* This is a simple demo program showing how
* to read a text file in C.
*
* Created 13 October 2015 by Sally Goldin for CPE100
*/

#include <stdio.h>
#include <stdlib.h> /* for exit fn */
int main()
{
    FILE* pIn = NULL;
    char filename[256];
    char inputline[256];
    char response;
    int count;
    printf("What file do you want to read? ");
    fgets(inputline,sizeof(inputline),stdin);
    sscanf(inputline,"%s",filename);
    pIn = fopen(filename,"r");
    if (pIn == NULL)
    {
        printf("Error opening file: %s\n", filename);
        exit(1);
    }
    count = 0;
    while (fgets(inputline,sizeof(inputline),pIn) != NULL)
    {
        printf("Read: %s",inputline);
        count++;
        if (count % 5 == 0)
        {
            printf("Want to stop (Y/N)? ");
            fgets(inputline,sizeof(inputline),stdin);
            sscanf(inputline,"%c",&response);
            if ((response == 'Y') || (response == 'y'))
            break;
        }
    }
    fclose(pIn);
}

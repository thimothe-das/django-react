from .serializers import PostSerializer
from .models import Post
from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
from rest_framework import status
import csv
import os
import boto3
from pandas import DataFrame, read_csv, ExcelWriter
import pandas as pd
import xlwt
# Create your views here.

class PostView(APIView):
    parser_classes = (MultiPartParser, FormParser)

    def get(self, request, *args, **kwargs):
        posts = Post.objects.all()
        serializer = PostSerializer(posts, many=True)
        return Response(serializer.data)

    def post(self, request, *args, **kwargs):
        posts_serializer = PostSerializer(data=request.data)
        QueryDict = request.data
        object_dict = QueryDict.dict()
        file_name = str(object_dict['file'])
        file_realname = file_name.replace(" ", "_")
        file_realnames = file_realname.replace("̀", "")
        if posts_serializer.is_valid():
            posts_serializer.save()

            print('working until here')
            df = pd.read_excel('./file/post_images/' + file_realnames, sheet_name='BAREME-MEASUREMENT CHART', header=1)
            df = df.drop(df.columns[[2]], axis=1)
            df = df.iloc[:, :-8]
           
            if file_name.startswith('01001-5934') == True:
                searchfor = ['Tour de poitrine', 'Tour de taille', 'Longueur de manche ML', '65cm']

            elif file_name.startswith('01804-8230') == True:
                searchfor = ['Tour de taille étirée', 'Tour de taille', 'Tour de bassin', '1/2 Tour de  cuisse à 2.5cm', 'Longueur d\'entrejambe']

            elif file_name.startswith('00400-0270') == True:
                searchfor = ['1/2 Tour de poitrine à 2,5 cms\n1/2 Chest', 'Tour de taille', 'Longueur de manche raglan dépliée', '65cm']

            elif file_name.startswith('01804-3921') == True:
                searchfor = ['( elastique à partir du 44)', '1/2 Tour de taille étirée\n1/2 Waistround', '1/2 Tour de bassin\n1/2 Hips round', '1/2 Tour de  cuisse à 2.5cm\n1/2 Thigh round', 'Longueur d\'entrejambe' + '\n' + 'Inleg length']

            elif file_name.startswith('01804-3922') == True:
                searchfor = ['( elastique à partir du 44)', '1/2 Tour de taille étirée\n1/2 Waistround', '1/2 Tour de bassin\n1/2 Hips round', '1/2 Tour de  cuisse à 2.5cm\n1/2 Thigh round', 'Longueur d\'entrejambe' + '\n' + 'Inleg length']


            df = df[df['TAILLES FRANCAISES '].str.contains('|'.join(searchfor)) ]
            writer = ExcelWriter('export' + file_realname)
            df.to_excel(writer)
            writer.save()

            s3 = boto3.client('s3')
            bucket_name = 'compartiment-thimothe'
            print('jusquala')
            s3.upload_file('export' + str(file_realname), bucket_name, 'export' + str(file_realname))
            print('Successfully uploaded')



            return Response(posts_serializer.data, status=status.HTTP_201_CREATED)
        else:
            print('error', posts_serializer.errors)
            return Response(posts_serializer.errors, status=status.HTTP_400_BAD_REQUEST)